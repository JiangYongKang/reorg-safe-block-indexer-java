package com.github.highcumontoa.reorgsafeblockindexerjava.service;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.ChainSnapshot;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SourceAttempt;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSourceException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 区块索引服务。
 *
 * <p>核心规则：
 * <ol>
 *   <li><b>串行变更</b>：sync 与 submitBlock 共用一把互斥锁；sync 用 tryLock，
 *       并发触发只有一个真正执行，其余得到 {@link SyncStatus#BUSY_SKIPPED}。</li>
 *   <li><b>先拉取后提交</b>：从各候选源整链取数并形成跨源共识之前不写任何状态；
 *       某个源中途失败仅排除该源，来源分歧则不提交，本地无部分写入。</li>
 *   <li><b>重组原子化</b>：检测分叉点后，回滚与重放在存储层的私有草稿上一次完成，
 *       CAS 整体替换快照；对外查询永远不会看到中间态。</li>
 *   <li><b>同高分歧不盲选</b>：多个最高高度相同的来源必须在<b>每一个高度</b>上哈希一致才算
 *       “同一条链”；只要存在分歧就返回 {@link SyncStatus#SOURCES_DIVERGED}，不采用任何来源，
 *       本地保持不动，等来源重新一致后再推进。</li>
 *   <li><b>等长链按重组切换</b>：来源一致链与本地<b>等长但内容不同</b>时，不做幂等跳过，
 *       而是按一次链切换处理——回滚作废段、重放共识链；水位不增不减，提交仍然原子。
 *       仅当来源一致链严格短于本地（全部来源落后）时才拒绝。</li>
 *   <li><b>失败可解释</b>：TIMEOUT / TRANSPORT / BUSINESS 三类错误分别记录来源，
 *       全部源失败时返回 {@link SyncStatus#ALL_SOURCES_UNAVAILABLE} 及逐源尝试明细。</li>
 * </ol>
 */
public class BlockIndexer {

    private static final Logger log = LoggerFactory.getLogger(BlockIndexer.class);

    private final List<BlockSource> sources;
    private final IndexStore store;
    private final ReentrantLock mutationLock = new ReentrantLock();

    public BlockIndexer(List<BlockSource> sources, IndexStore store) {
        this.sources = List.copyOf(sources);
        this.store = store;
    }

    /** 触发一次增量同步。并发触发安全：进行中的其它调用立即得到 BUSY_SKIPPED。 */
    public SyncResult sync() {
        if (!mutationLock.tryLock()) {
            long w = store.watermark();
            log.info("同步触发被跳过：已有同步在进行中，当前水位={}", w);
            return new SyncResult(SyncStatus.BUSY_SKIPPED, w, w, -1,
                    List.of(), List.of(), List.of(), "已有同步在进行中，本次触发已跳过");
        }
        try {
            return doSync();
        } finally {
            mutationLock.unlock();
        }
    }

    private SyncResult doSync() {
        ChainSnapshot snap = store.snapshot();
        long before = snap.watermark();
        log.info("开始同步：当前水位={}，候选数据源={}", before, sourceNames());

        // 阶段一：向所有源探测链头高度。
        Map<String, Long> heights = new LinkedHashMap<>();
        Map<String, SourceAttempt> probeFailures = new LinkedHashMap<>();
        long maxHeight = Long.MIN_VALUE;
        for (BlockSource src : sources) {
            try {
                long h = src.headHeight();
                heights.put(src.name(), h);
                maxHeight = Math.max(maxHeight, h);
            } catch (BlockSourceException e) {
                probeFailures.put(src.name(),
                        new SourceAttempt(src.name(), false, e.kind(), e.getMessage()));
                log.warn("数据源 [{}] 链头探测失败：kind={}，原因={}（无任何本地写入）",
                        src.name(), e.kind(), e.getMessage());
            }
        }

        // 槽位与源一一对应，保证 attempts 严格按配置优先级呈现。
        List<SourceAttempt> attempts = new ArrayList<>();
        List<BlockSource> candidates = new ArrayList<>();
        for (BlockSource src : sources) {
            int slot = attempts.size();
            attempts.add(null);
            SourceAttempt probeFailure = probeFailures.get(src.name());
            if (probeFailure != null) {
                attempts.set(slot, probeFailure);
                continue;
            }
            long h = heights.get(src.name());
            if (h < maxHeight) {
                attempts.set(slot, new SourceAttempt(src.name(), false, "BUSINESS",
                        "源高度 " + h + " 落后于其它源最高高度 " + maxHeight
                                + "，能力不足，跳过该源"));
                log.warn("数据源 [{}] 能力不足：高度 {} < 最高高度 {}", src.name(), h, maxHeight);
            } else {
                candidates.add(src);
            }
        }

        if (candidates.isEmpty()) {
            return allUnavailable(before, attempts, "所有数据源探测均失败");
        }

        // 阶段二：从每个最高高度候选源整链取数（先拉取后提交，中途失败不写本地）。
        // 取数成功的源按“每个高度的哈希序列”分组；序列完全相同才是同一条链。
        Map<List<String>, List<BlockSource>> groups = new LinkedHashMap<>();
        Map<BlockSource, List<Block>> fetchedBySource = new LinkedHashMap<>();
        for (BlockSource src : candidates) {
            int slot = findSlot(attempts, src);
            try {
                List<Block> chain = fetchFullChain(src, maxHeight);
                fetchedBySource.put(src, chain);
                groups.computeIfAbsent(hashesOf(chain), k -> new ArrayList<>()).add(src);
                attempts.set(slot, null); // 共识结论阶段再回填
            } catch (BlockSourceException e) {
                attempts.set(slot, new SourceAttempt(src.name(), false, e.kind(), e.getMessage()));
                log.warn("数据源 [{}] 取数失败：kind={}，原因={}（无任何本地写入）",
                        src.name(), e.kind(), e.getMessage());
            }
        }

        // 阶段三：来源之间在同一高度给出不同链——不挑任何一边，本地保持原样。
        if (groups.size() > 1) {
            return buildDivergence(before, maxHeight, groups, fetchedBySource, attempts);
        }
        if (groups.isEmpty()) {
            return allUnavailable(before, attempts, "所有数据源取数均失败");
        }

        // 唯一一组 = 来源共识链。成功标记在 applyConsensus 真正成功的返回点回填，
        // 若共识链因落后被拒绝，这些槽位要改填 BUSINESS，不能提前记成功。
        Map.Entry<List<String>, List<BlockSource>> consensus = groups.entrySet().iterator().next();
        List<BlockSource> agreeing = consensus.getValue();
        List<Block> canonical = fetchedBySource.get(agreeing.get(0));
        return applyConsensus(agreeing, snap, before, canonical, maxHeight, attempts);
    }

    /**
     * 把来源共识链与本地链比对并原子提交：
     * <ul>
     *   <li>共识链严格短于本地（全部来源落后）：拒绝，不允许水位倒退；</li>
     *   <li>等长且完全一致：ALREADY_SYNCED；</li>
     *   <li>等长但内容不同（含深度分叉）：REORGANIZED，回滚作废段并重放共识链；</li>
     *   <li>更长：定位分叉点，追加或重组推进。</li>
     * </ul>
     */
    private SyncResult applyConsensus(List<BlockSource> agreeing, ChainSnapshot snap, long before,
                                      List<Block> canonical, long head,
                                      List<SourceAttempt> attempts) {
        BlockSource chosen = agreeing.get(0);
        if (head < before) {
            for (int i = 0; i < attempts.size(); i++) {
                if (attempts.get(i) == null) {
                    BlockSource src = sources.get(i);
                    attempts.set(i, new SourceAttempt(src.name(), false, "BUSINESS",
                            "共识链高度 " + head + " 落后于本地水位 " + before
                                    + "，不允许水位倒退"));
                }
            }
            return allUnavailable(before, attempts,
                    "来源一致链高度 " + head + " 落后于本地水位 " + before + "，拒绝切换");
        }

        if (before == -1 && head == before) {
            log.info("同步幂等跳过：本地与来源共识均为空链");
            markAgreeingSources(agreeing, attempts, "空链已同步");
            return new SyncResult(SyncStatus.ALREADY_SYNCED, before, before, -1,
                    List.of(), List.of(), List.copyOf(attempts), "本地与源均为空链");
        }

        // 等长或更长都一样：从本地水位沿高度向下与共识链逐块比对哈希，定位共同祖先。
        // 等长且全部一致时返回 before+1：回滚段与重放段都为空 -> 幂等跳过。
        long forkHeight = before < 0 ? 0L : locateForkHeight(canonical, snap, before);

        List<String> rolledBack = new ArrayList<>();
        for (long h = forkHeight; h <= before; h++) {
            rolledBack.add(snap.blocks().get((int) h).hash());
        }
        List<Block> incoming = canonical.subList((int) forkHeight, canonical.size());
        List<String> applied = hashesOf(incoming);

        // 等长但内容一致：没有任何需要回滚或重放的区块。
        boolean identical = rolledBack.isEmpty() && applied.isEmpty();
        if (identical) {
            log.info("同步幂等跳过：水位={}，tip={}", before,
                    snap.blocks().get((int) before).hash());
            markAgreeingSources(agreeing, attempts, "head=" + head + " 已同步");
            return new SyncResult(SyncStatus.ALREADY_SYNCED, before, before, -1,
                    List.of(), List.of(), List.copyOf(attempts),
                    "水位 " + before + " 已与来源共识一致");
        }

        ChainSnapshot next = store.commit(snap, forkHeight, incoming)
                .orElseThrow(() -> new IllegalStateException(
                        "快照 CAS 提交失败：锁内不应发生，请检查存储实现"));

        markAgreeingSources(agreeing, attempts,
                "head=" + head + ", forkHeight=" + forkHeight);

        SyncStatus status = SyncStatus.REORGANIZED;
        if (head > before && forkHeight > before) {
            status = SyncStatus.ADVANCED; // 纯追加
        }

        if (status == SyncStatus.REORGANIZED) {
            log.info("链切换/重组已提交：数据源={}，水位 {} -> {}，回滚高度区间=[{}, {}]，"
                            + "回滚哈希={}，重放哈希={}",
                    chosen.name(), before, next.watermark(), forkHeight, before, rolledBack, applied);
        } else {
            log.info("同步推进已提交：数据源={}，水位 {} -> {}，新增哈希={}",
                    chosen.name(), before, next.watermark(), applied);
        }

        String msg;
        if (head == before) {
            msg = "来源在同一水位 " + before + " 给出不同链，已按链切换回滚 "
                    + rolledBack.size() + " 个区块并重放 " + applied.size() + " 个区块";
        } else if (status == SyncStatus.REORGANIZED) {
            msg = "在高度 " + forkHeight + " 分叉，已回滚 " + rolledBack.size()
                    + " 个区块并重放 " + applied.size() + " 个区块";
        } else {
            msg = "新增 " + applied.size() + " 个区块";
        }
        return new SyncResult(status, before, next.watermark(),
                status == SyncStatus.REORGANIZED ? forkHeight : -1,
                List.copyOf(rolledBack), List.copyOf(applied),
                List.copyOf(attempts), msg);
    }

    /**
     * 形成 SOURCES_DIVERGED 结论：按配置顺序取第一组为基准，逐高度定位首个分歧点，
     * 回填每个候选源的尝试明细。结论确定、可重复，本地不发生任何写入。
     */
    private SyncResult buildDivergence(long before, long head,
                                       Map<List<String>, List<BlockSource>> groups,
                                       Map<BlockSource, List<Block>> fetchedBySource,
                                       List<SourceAttempt> attempts) {
        List<Block> anchorChain = fetchedBySource.get(groups.values().iterator().next().get(0));
        long firstDiff = -1;
        for (long h = 0; h <= head; h++) {
            String anchorHash = anchorChain.get((int) h).hash();
            for (List<Block> chain : fetchedBySource.values()) {
                if (!chain.get((int) h).hash().equals(anchorHash)) {
                    firstDiff = h;
                    break;
                }
            }
            if (firstDiff >= 0) {
                break;
            }
        }

        for (Map.Entry<List<String>, List<BlockSource>> group : groups.entrySet()) {
            List<Block> chain = fetchedBySource.get(group.getValue().get(0));
            boolean anchor = chain == anchorChain;
            String detail = (anchor ? "同高分歧基准链：首个分歧高度=" + firstDiff
                    : "与基准链在高度 " + firstDiff + " 处哈希不同")
                    + "，tip=" + chain.get((int) head).hash();
            for (BlockSource src : group.getValue()) {
                int slot = findSlot(attempts, src);
                attempts.set(slot, new SourceAttempt(src.name(), false, "DIVERGENCE", detail));
            }
        }

        String names = summarize(attempts);
        String message = "最高高度均为 " + head + " 的数据源在高度 " + firstDiff
                + " 处内容互相矛盾，无法判定规范链，本地保持水位 " + before + " 不动: " + names;
        log.warn("数据源之间同高分歧：首个分歧高度={}，水位保持={}，逐源结论={}",
                firstDiff, before, names);
        return new SyncResult(SyncStatus.SOURCES_DIVERGED, before, before, -1,
                List.of(), List.of(), List.copyOf(attempts), message);
    }

    private void markAgreeingSources(List<BlockSource> agreeing, List<SourceAttempt> attempts,
                                     String baseDetail) {
        for (int i = 0; i < agreeing.size(); i++) {
            BlockSource src = agreeing.get(i);
            int slot = findSlot(attempts, src);
            String detail = i == 0 ? baseDetail : baseDetail + "；与共识链逐高度一致";
            attempts.set(slot, new SourceAttempt(src.name(), true, null, detail));
        }
    }

    private int findSlot(List<SourceAttempt> attempts, BlockSource src) {
        for (int i = 0; i < sources.size(); i++) {
            if (sources.get(i).name().equals(src.name())) {
                return i;
            }
        }
        throw new IllegalStateException("未知数据源: " + src.name());
    }

    private SyncResult allUnavailable(long before, List<SourceAttempt> attempts, String reason) {
        SyncResult fail = new SyncResult(SyncStatus.ALL_SOURCES_UNAVAILABLE, before, before, -1,
                List.of(), List.of(), List.copyOf(attempts),
                reason + ": " + summarize(attempts));
        log.error("同步失败：水位保持={}，逐源结论={}", before, summarize(attempts));
        return fail;
    }

    /**
     * 从来源整链取 [0, head]，并在内存中校验高度连续、父子相接。
     * 任何缺失/异常/不自洽都在提交前抛出，调用方放弃该源，本地零写入。
     */
    private List<Block> fetchFullChain(BlockSource src, long head) throws BlockSourceException {
        List<Block> chain = new ArrayList<>();
        String expectedParent = null;
        for (long h = 0; h <= head; h++) {
            Block block = requireBlock(src, h);
            if (h > 0 && !block.parentHash().equals(expectedParent)) {
                throw new SourceBusinessException(src.name() + ": 高度 " + h
                        + " 的区块未接在高度 " + (h - 1) + " 之后（父哈希不匹配），源链不自洽");
            }
            chain.add(block);
            expectedParent = block.hash();
        }
        return List.copyOf(chain);
    }

    private List<String> hashesOf(List<Block> blocks) {
        List<String> hashes = new ArrayList<>(blocks.size());
        for (Block b : blocks) {
            hashes.add(b.hash());
        }
        return List.copyOf(hashes);
    }

    /**
     * 从本地水位沿高度向下与已取回的共识链比对哈希，返回首个不一致高度
     * （即分叉后第一块的高度）。全部一致（纯追加/完全相同）返回 before + 1；
     * 创世即分叉返回 0。
     */
    private long locateForkHeight(List<Block> canonical, ChainSnapshot snap, long before) {
        for (long h = Math.min(before, canonical.size() - 1L); h >= 0; h--) {
            if (canonical.get((int) h).hash().equals(snap.blocks().get((int) h).hash())) {
                return h + 1;
            }
        }
        return 0;
    }

    private Block requireBlock(BlockSource src, long height) throws BlockSourceException {
        Optional<Block> b = src.blockAtHeight(height);
        if (b.isEmpty()) {
            throw new SourceBusinessException(
                    src.name() + ": 高度 " + height + " 的区块缺失，源数据不完整");
        }
        Block block = b.get();
        if (block.height() != height) {
            throw new SourceBusinessException(
                    src.name() + ": 高度 " + height + " 返回的区块高度字段为 " + block.height());
        }
        return block;
    }

    /**
     * 外部直接投递单个区块（可能重复、乱序或属于分叉）。
     * 只接受紧接当前水位的下一个区块；重复区块幂等跳过；其余拒绝且不写入。
     */
    public SyncResult submitBlock(Block block) {
        mutationLock.lock();
        try {
            ChainSnapshot snap = store.snapshot();
            long before = snap.watermark();

            if (block.height() <= before) {
                Optional<Block> existing = store.blockAtHeight(block.height());
                if (existing.isPresent() && existing.get().hash().equals(block.hash())) {
                    log.info("重复区块投递已忽略：高度={}，hash={}，水位保持={}",
                            block.height(), block.hash(), before);
                    return new SyncResult(SyncStatus.ALREADY_SYNCED, before, before, -1,
                            List.of(), List.of(), List.of(),
                            "区块 " + block.hash() + " 已存在，幂等忽略");
                }
                log.info("冲突/历史区块投递被拒绝：高度={}，hash={}，水位={}",
                        block.height(), block.hash(), before);
                return new SyncResult(SyncStatus.REJECTED, before, before, -1,
                        List.of(), List.of(), List.of(),
                        "高度 " + block.height() + " 的冲突区块不能直接投递，"
                                + "分叉重放须通过 sync 完成；未产生写入");
            }

            if (block.height() > before + 1) {
                log.info("超前区块投递被拒绝：高度={}，当前水位={}（存在缺口）",
                        block.height(), before);
                return new SyncResult(SyncStatus.REJECTED, before, before, -1,
                        List.of(), List.of(), List.of(),
                        "缺少高度 " + (before + 1) + "，不能直接跳到 " + block.height()
                                + "；未产生写入");
            }

            // block.height() == before + 1
            if (before >= 0) {
                String tipHash = snap.blocks().get((int) before).hash();
                if (!block.parentHash().equals(tipHash)) {
                    log.info("孤块投递被拒绝：高度={}，父哈希={}，当前tip={}",
                            block.height(), block.parentHash(), tipHash);
                    return new SyncResult(SyncStatus.REJECTED, before, before, -1,
                            List.of(), List.of(), List.of(),
                            "区块未接在当前 tip " + tipHash + " 之后；未产生写入");
                }
            }

            ChainSnapshot next = store.commit(snap, before + 1, List.of(block))
                    .orElseThrow(() -> new IllegalStateException(
                            "快照 CAS 提交失败：锁内不应发生"));
            log.info("投递区块已提交：高度={}，hash={}，水位 {} -> {}",
                    block.height(), block.hash(), before, next.watermark());
            return new SyncResult(SyncStatus.ADVANCED, before, next.watermark(), -1,
                    List.of(), List.of(block.hash()), List.of(),
                    "区块 " + block.hash() + " 已索引");
        } finally {
            mutationLock.unlock();
        }
    }

    /** 当前链的只读快照（同步进行中调用安全，只可能读到旧的完整快照）。 */
    public ChainSnapshot snapshot() {
        return store.snapshot();
    }

    public long watermark() {
        return store.watermark();
    }

    private List<String> sourceNames() {
        List<String> names = new ArrayList<>();
        for (BlockSource s : sources) {
            names.add(s.name());
        }
        return names;
    }

    private String summarize(List<SourceAttempt> attempts) {
        StringBuilder sb = new StringBuilder();
        for (SourceAttempt a : attempts) {
            if (!sb.isEmpty()) {
                sb.append("; ");
            }
            sb.append(a.sourceName()).append('=').append(a.success() ? "OK" : a.failureKind())
                    .append('(').append(a.detail()).append(')');
        }
        return sb.toString();
    }
}
