package com.github.highcumontoa.reorgsafeblockindexerjava.service;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.ChainState;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.SyncWatermark;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTimeoutException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTransportException;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.InMemoryIndexStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 区块索引核心服务。
 * <p>一致性保证：
 * <ul>
 *   <li>同步由单把重入锁串行化，并发触发的同步排队执行，重复同步/重复投递幂等；</li>
 *   <li>链重组时先在工作区计算回滚点与新链、完成全部校验，再构造完整 {@link ChainState} 原子发布，
 *       查询永远读到「上一个」或「下一个」完整快照，读不阻塞写，也读不到回滚中间态；</li>
 *   <li>任何数据源失败或跨源不一致都只返回失败结论，绝不发布半成品状态；
 *       失败时水位保持旧值，水位（高度）单调不降。</li>
 * </ul>
 */
public final class BlockIndexerService {

    private static final Logger log = LoggerFactory.getLogger(BlockIndexerService.class);

    private final InMemoryIndexStore store;
    private final SourceRouter router;
    private final ReentrantLock syncLock = new ReentrantLock();

    public BlockIndexerService(InMemoryIndexStore store, SourceRouter router) {
        this.store = store;
        this.router = router;
    }

    /**
     * 执行一次增量同步（并发安全、可重复调用）。
     * 失败时返回可解释的 {@link SyncResult}，不抛出运行时异常。
     */
    public SyncResult syncOnce() {
        syncLock.lock();
        try {
            ChainState current = store.snapshot();
            SyncWatermark before = current.watermark();
            log.info("sync start: watermark before = {}", fmt(before));

            // 聚合所有源各自报告的链头，而不是只看优先级最高的源：
            // 滞后源不应把同步高度拉低；同高度链头哈希不一致则拒绝同步。
            HeadSurvey survey = surveyHeads();
            if (!survey.failures().isEmpty() && survey.heads().isEmpty()) {
                return failAndLog(before,
                        SourceRouter.Routed.fail(survey.failures(),
                                dominantOf(survey.failures())));
            }
            if (survey.heads().isEmpty()) {
                log.info("sync no-op: no source has any block yet, watermark stays {}", fmt(before));
                return SyncResult.success(before, before, -1, -1, -1, null, false,
                        "no source has blocks");
            }

            Block head = survey.chosenHead();
            if (head.height() < before.height()) {
                return businessFail(before,
                        "source tip height " + head.height() + " is behind local watermark "
                                + before.height() + "; refusing to regress watermark",
                        List.of());
            }
            if (head.height() == before.height() && head.hash().equals(before.tipHash())) {
                log.info("sync duplicate: head {}@{} already indexed, watermark stays {}",
                        head.hash(), head.height(), fmt(before));
                return SyncResult.success(before, before, -1, -1, -1,
                        survey.chosenSourceName(), true, "already at source tip");
            }

            SyncResult disagreement = detectHeadDivergence(before, survey);
            if (disagreement != null) {
                return disagreement;
            }

            return applyCandidate(before, head, survey.chosenSourceName());
        } finally {
            syncLock.unlock();
        }
    }

    /**
     * 直接投递/去重接收一个区块（消息重复投递场景）。
     * <ul>
     *   <li>区块已在规范链中：幂等忽略，duplicate=true；</li>
     *   <li>与已确认区块同高度但哈希不同 / 父链接不上：按重组处理，回滚后应用；</li>
     *   <li>高度超过当前 tip+1 造成缺口：返回 BUSINESS 失败，不写入。</li>
     * </ul>
     */
    public SyncResult ingest(Block block) {
        if (block == null || block.hash() == null) {
            return businessFail(store.watermark(), "ingested block must have a hash", List.of());
        }
        syncLock.lock();
        try {
            ChainState current = store.snapshot();
            SyncWatermark before = current.watermark();
            log.info("ingest start: block {}@{} watermark before = {}",
                    block.hash(), block.height(), fmt(before));

            if (current.containsHash(block.hash())) {
                log.info("ingest duplicate: block {}@{} already indexed, watermark stays {}",
                        block.hash(), block.height(), fmt(before));
                return SyncResult.success(before, before, -1, -1, -1, "delivery", true,
                        "duplicate delivery ignored");
            }
            if (block.height() > before.height() + 1) {
                return businessFail(before,
                        "gap detected: delivered height " + block.height()
                                + " exceeds local tip+1 (" + (before.height() + 1) + ")",
                        List.of());
            }
            if (block.height() < 0) {
                return businessFail(before, "negative block height: " + block.height(), List.of());
            }

            // 以投递区块为链头候选；其余区块从数据源补齐，且数据源必须与投递内容一致。
            Block head = block;
            SourceRouter.Routed<Block> corroboration = router.fetchBlock(head.height());
            if (!corroboration.success()) {
                return failAndLog(before, corroboration);
            }
            if (!corroboration.value().hash().equals(head.hash())) {
                SourceFailure sf = new SourceFailure(corroboration.source().capabilities().name(),
                        FailureType.BUSINESS,
                        "delivered block " + head.hash() + " disagrees with source block "
                                + corroboration.value().hash() + " at height " + head.height());
                return businessFail(before,
                        "delivered block is not corroborated by data source", List.of(sf));
            }
            return applyCandidate(before, head,
                    "delivery+" + corroboration.source().capabilities().name());
        } finally {
            syncLock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // internal: cross-source agreement & chain application
    // ------------------------------------------------------------------

    /** 一次链头聚合的结果：各源链头、所选（最高）链头及其来源、以及各源失败记录。 */
    private record HeadSurvey(List<SeenHead> heads, Block chosenHead, String chosenSourceName,
                              List<SourceFailure> failures) {
    }

    /** 某个数据源报告的链头。 */
    private record SeenHead(Block block, String sourceName) {
    }

    /**
     * 轮询所有数据源的链头：
     * <ul>
     *   <li>传输类失败仅记录，不影响其他源（后续区块拉取时路由器还会再次尝试切换）；</li>
     *   <li>选择报告高度最高的链头作为候选，来源取该链头的报告源中优先级最高者；</li>
     *   <li>同一最高高度出现不同哈希的链头时，由 {@link #detectHeadDivergence} 判定为分歧。</li>
     * </ul>
     */
    private HeadSurvey surveyHeads() {
        List<SeenHead> heads = new ArrayList<>();
        List<SourceFailure> failures = new ArrayList<>();
        for (BlockSource s : router.sources()) {
            try {
                Optional<Block> h = s.head();
                h.ifPresent(block -> heads.add(new SeenHead(block, s.capabilities().name())));
            } catch (SourceBusinessException e) {
                failures.add(new SourceFailure(s.capabilities().name(), FailureType.BUSINESS,
                        e.getMessage()));
            } catch (SourceTimeoutException e) {
                failures.add(new SourceFailure(s.capabilities().name(), FailureType.TIMEOUT,
                        e.getMessage()));
            } catch (SourceTransportException e) {
                failures.add(new SourceFailure(s.capabilities().name(), FailureType.TRANSPORT,
                        e.getMessage()));
            }
        }
        if (heads.isEmpty()) {
            return new HeadSurvey(List.of(), null, null, List.copyOf(failures));
        }
        long maxHeight = heads.stream().mapToLong(x -> x.block().height()).max().orElse(-1);
        // sources 已按 priority 升序，第一个即最高优先级
        SeenHead chosen = heads.stream()
                .filter(x -> x.block().height() == maxHeight)
                .findFirst()
                .orElseThrow();
        return new HeadSurvey(List.copyOf(heads), chosen.block(), chosen.sourceName(),
                List.copyOf(failures));
    }

    /** 同高度链头哈希不一致即判定数据源分歧，拒绝同步，避免污染本地状态。 */
    private SyncResult detectHeadDivergence(SyncWatermark before, HeadSurvey survey) {
        Block candidate = survey.chosenHead();
        List<SourceFailure> disagreements = new ArrayList<>();
        for (SeenHead seen : survey.heads()) {
            Block other = seen.block();
            if (other.height() == candidate.height()
                    && !other.hash().equals(candidate.hash())) {
                disagreements.add(new SourceFailure(seen.sourceName(),
                        FailureType.BUSINESS,
                        "head divergence at height " + candidate.height() + ": source="
                                + other.hash() + ", chosen=" + candidate.hash()));
            }
        }
        if (!disagreements.isEmpty()) {
            return businessFail(before,
                    "data sources diverge at the same tip height; local state left untouched",
                    disagreements);
        }
        return null;
    }

    private FailureType dominantOf(List<SourceFailure> failures) {
        int business = 0;
        int transport = 0;
        int timeout = 0;
        for (SourceFailure f : failures) {
            switch (f.type()) {
                case BUSINESS -> business++;
                case TRANSPORT -> transport++;
                case TIMEOUT -> timeout++;
                default -> { }
            }
        }
        if (business >= transport && business >= timeout && business > 0) {
            return FailureType.BUSINESS;
        }
        if (transport >= timeout && transport > 0) {
            return FailureType.TRANSPORT;
        }
        if (timeout > 0) {
            return FailureType.TIMEOUT;
        }
        return FailureType.NO_SOURCE;
    }

    private SyncResult applyCandidate(SyncWatermark before, Block head, String sourceName) {
        ChainState current = store.snapshot();
        long tipHeight = head.height();

        // 拉取候选链的完整区块（0..tip）。
        ChainFetch fetched = fetchFullChain(tipHeight);
        if (!fetched.success()) {
            return failAndLog(before, fetched.blockFailure());
        }
        List<Block> candidateChain = fetched.blocks();

        Block tip = candidateChain.get((int) tipHeight);
        if (!tip.hash().equals(head.hash())) {
            return businessFail(before,
                    "fetched tip " + tip.hash() + " does not match announced head " + head.hash(),
                    List.of());
        }

        // 候选链结构校验：连续、父哈希闭合。
        try {
            validateChain(candidateChain);
        } catch (SourceBusinessException e) {
            return businessFail(before, e.getMessage(), List.of());
        }

        // 找分叉点：本地与候选链最高的共有高度且哈希相同。
        long localTip = current.tipHeight();
        long fork = -1;
        long sharedTop = Math.min(localTip, tipHeight);
        for (long h = sharedTop; h >= 0; h--) {
            Block local = current.blockAt(h).orElse(null);
            Block remote = candidateChain.get((int) h);
            if (local != null && local.hash().equals(remote.hash())) {
                fork = h;
                break;
            }
        }
        if (localTip >= 0 && fork < 0) {
            return businessFail(before,
                    "no common ancestor between local chain and source chain; refusing overwrite",
                    List.of());
        }

        // 回滚范围（仅用于日志与结果说明，实际新快照直接丢弃这部分）。
        long rollbackFrom = localTip > fork ? fork + 1 : -1;
        long rollbackTo = localTip > fork ? localTip : -1;
        if (rollbackFrom >= 0) {
            log.warn("REORG detected via {}: rollback heights [{}, {}] ({} blocks) "
                            + "before applying new chain",
                    sourceName, rollbackFrom, rollbackTo, rollbackTo - rollbackFrom + 1);
        }

        // 组装新链：本地保留前缀 + 候选新后缀；跨边界父链接校验已由 validateChain 覆盖。
        List<Block> merged = new ArrayList<>();
        for (long h = 0; h <= fork; h++) {
            merged.add(current.blockAt(h).orElseThrow());
        }
        for (long h = fork + 1; h <= tipHeight; h++) {
            merged.add(candidateChain.get((int) h));
        }
        try {
            validateChain(merged);
        } catch (SourceBusinessException e) {
            return businessFail(before, e.getMessage(), List.of());
        }

        long appliedFrom = fork + 1;
        long appliedTo = tipHeight;
        ChainState newState = ChainState.from(merged);
        store.publish(newState); // 唯一的写入点：一次性原子发布
        SyncWatermark after = newState.watermark();
        log.info("sync committed via {}: watermark {} -> {}, applied heights [{}, {}], "
                        + "rollback [{}, {}]",
                sourceName, fmt(before), fmt(after), appliedFrom, appliedTo,
                rollbackFrom, rollbackTo);
        return SyncResult.success(before, after, rollbackFrom, appliedFrom, appliedTo,
                sourceName, appliedFrom > appliedTo,
                rollbackFrom >= 0 ? "reorg applied" : "extended");
    }

    /** 优先批量拉取整链；批量不可用/失败时退化为逐高度路由拉取。 */
    private ChainFetch fetchFullChain(long tipHeight) {
        SourceRouter.Routed<List<Block>> batch = router.fetchBlocks(0, tipHeight);
        if (batch.success()) {
            return ChainFetch.ok(batch.value());
        }
        // 批量路径整体失败（可能仅因为无人支持批量）；退化为逐块拉取。
        List<Block> blocks = new ArrayList<>();
        for (long h = 0; h <= tipHeight; h++) {
            SourceRouter.Routed<Block> one = router.fetchBlock(h);
            if (!one.success()) {
                return ChainFetch.failed(one);
            }
            blocks.add(one.value());
        }
        return ChainFetch.ok(blocks);
    }

    /** 整链拉取结果：成功携带区块，失败携带最后一次路由结论。 */
    private record ChainFetch(boolean success, List<Block> blocks,
                              SourceRouter.Routed<Block> blockFailure) {
        static ChainFetch ok(List<Block> blocks) {
            return new ChainFetch(true, blocks, null);
        }

        static ChainFetch failed(SourceRouter.Routed<Block> failure) {
            return new ChainFetch(false, List.of(), failure);
        }
    }

    private void validateChain(List<Block> chain) {
        if (chain.isEmpty()) {
            return;
        }
        Block first = chain.get(0);
        if (first.height() != 0 || first.parentHash() != null) {
            throw new SourceBusinessException("local-index",
                    "invalid genesis: " + first);
        }
        for (int i = 1; i < chain.size(); i++) {
            Block prev = chain.get(i - 1);
            Block cur = chain.get(i);
            if (cur.height() != prev.height() + 1) {
                throw new SourceBusinessException("local-index",
                        "non-contiguous chain between heights " + prev.height()
                                + " and " + cur.height());
            }
            if (!prev.hash().equals(cur.parentHash())) {
                throw new SourceBusinessException("local-index",
                        "broken parent link at height " + cur.height());
            }
        }
    }

    // ------------------------------------------------------------------
    // failure helpers
    // ------------------------------------------------------------------

    private SyncResult failAndLog(SyncWatermark before, SourceRouter.Routed<?> routed) {
        FailureType type = routed.dominantFailureType();
        List<SourceFailure> failures = routed.failures();
        for (SourceFailure f : failures) {
            log.warn("sync source failure: source={}, type={}, detail={}",
                    f.sourceName(), f.type(), f.message());
        }
        String message = "sync failed (" + type + ") across " + failures.size()
                + " source(s); local state untouched, watermark stays " + fmt(before);
        log.warn("sync abort: {}", message);
        return SyncResult.failure(before, type, message, failures);
    }

    private SyncResult businessFail(SyncWatermark before, String message,
                                    List<SourceFailure> sources) {
        for (SourceFailure f : sources) {
            log.warn("sync source failure: source={}, type={}, detail={}",
                    f.sourceName(), f.type(), f.message());
        }
        log.warn("sync abort (BUSINESS): {}; watermark stays {}", message, fmt(before));
        return SyncResult.failure(before, FailureType.BUSINESS, message, sources);
    }

    private static String fmt(SyncWatermark w) {
        if (w == null || w.height() < 0) {
            return "<empty:-1>";
        }
        return w.height() + "@" + w.tipHash();
    }

    // ------------------------------------------------------------------
    // reads
    // ------------------------------------------------------------------

    public ChainState snapshot() {
        return store.snapshot();
    }

    public SyncWatermark watermark() {
        return store.watermark();
    }

    public Optional<Block> blockAt(long height) {
        return store.blockAt(height);
    }

    public Optional<Block> findByHash(String hash) {
        return store.findByHash(hash);
    }

    public List<Block> range(long fromHeight, long toHeight) {
        return store.range(fromHeight, toHeight);
    }
}
