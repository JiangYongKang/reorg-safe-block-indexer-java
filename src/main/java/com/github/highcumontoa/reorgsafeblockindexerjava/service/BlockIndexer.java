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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 区块索引服务。
 *
 * <p>核心规则：
 * <ol>
 *   <li><b>串行变更</b>：sync 与 submitBlock 共用一把互斥锁；sync 用 tryLock，
 *       并发触发只有一个真正执行，其余得到 {@link SyncStatus#BUSY_SKIPPED}。</li>
 *   <li><b>先拉取后提交</b>：从某个数据源拉取全部所需区块成功之前不写任何状态；
 *       中途失败仅放弃该源并尝试下一个源，本地无部分写入。</li>
 *   <li><b>重组原子化</b>：检测分叉点后，回滚与重放在存储层的私有草稿上一次完成，
 *       CAS 整体替换快照；对外查询永远不会看到中间态。</li>
 *   <li><b>水位单调</b>：仅接受严格长于本地链的源；等长分叉与落后源被拒绝，
 *       重复同步得到 {@link SyncStatus#ALREADY_SYNCED}。</li>
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

        // 阶段一：向所有源探测链头高度。源间高度不一致时，只有最高高度的源可作为候选，
        // 落后源记为能力不足（BUSINESS），避免低高度/分叉数据污染本地状态。
        java.util.Map<String, Long> heights = new java.util.HashMap<>();
        java.util.Map<String, SourceAttempt> probeFailures = new java.util.LinkedHashMap<>();
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

        // 按配置优先级逐源占位：探测失败 / 高度落后的源立即给出结论，
        // 其余进入尝试阶段。槽位与源一一对应，保证结果严格按优先级顺序呈现。
        List<SourceAttempt> attempts = new ArrayList<>();
        List<Integer> candidateSlots = new ArrayList<>();
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
                candidateSlots.add(slot);
            }
        }

        // 阶段二：按优先级在最高高度候选源上依次尝试完整取数 + 原子提交。
        for (int i = 0; i < candidates.size(); i++) {
            BlockSource src = candidates.get(i);
            int slot = candidateSlots.get(i);
            try {
                return attemptSync(src, snap, before, attempts, slot, maxHeight);
            } catch (BlockSourceException e) {
                attempts.set(slot, new SourceAttempt(src.name(), false, e.kind(), e.getMessage()));
                log.warn("数据源 [{}] 同步失败：kind={}，原因={}（无任何本地写入）",
                        src.name(), e.kind(), e.getMessage());
            }
        }

        SyncResult fail = new SyncResult(SyncStatus.ALL_SOURCES_UNAVAILABLE, before, before, -1,
                List.of(), List.of(), List.copyOf(attempts),
                "所有数据源均不可用: " + summarize(attempts));
        log.error("同步失败：水位保持={}，逐源结论={}", before, summarize(attempts));
        return fail;
    }

    private SyncResult attemptSync(BlockSource src, ChainSnapshot snap, long before,
                                   List<SourceAttempt> attempts, int slot, long head)
            throws BlockSourceException {
        if (head < before) {
            throw new SourceBusinessException("源高度 " + head + " 落后于本地水位 " + before
                    + "，能力不足，跳过该源");
        }
        if (head == before) {
            // 高度持平：必须确认是同一条链，等长分叉不重放（最长链规则 + 水位单调）。
            if (before == -1) {
                attempts.set(slot, new SourceAttempt(src.name(), true, null, "空链已同步"));
                return new SyncResult(SyncStatus.ALREADY_SYNCED, before, before, -1,
                        List.of(), List.of(), List.copyOf(attempts), "本地与源均为空链");
            }
            Block sourceTip = requireBlock(src, before);
            Block localTip = snap.blocks().get((int) before);
            if (!sourceTip.hash().equals(localTip.hash())) {
                throw new SourceBusinessException("源在水位 " + before
                        + " 处为等长分叉（tip=" + sourceTip.hash() + "），不重放等长链");
            }
            attempts.set(slot, new SourceAttempt(src.name(), true, null,
                    "head=" + head + " 已同步"));
            log.info("同步幂等跳过：水位={}，tip={}", before, localTip.hash());
            return new SyncResult(SyncStatus.ALREADY_SYNCED, before, before, -1,
                    List.of(), List.of(), List.copyOf(attempts),
                    "水位 " + before + " 已与源一致");
        }

        // head > before：向源方向回溯，定位共同祖先。
        long forkHeight = locateForkHeight(src, snap, before);

        // 拉取 (forkHeight, head] 的完整新区段；任何缺失/异常都在提交前抛出，无部分写入。
        List<Block> incoming = new ArrayList<>();
        for (long h = forkHeight; h <= head; h++) {
            incoming.add(requireBlock(src, h));
        }

        List<String> rolledBack = new ArrayList<>();
        for (long h = forkHeight; h <= before; h++) {
            rolledBack.add(snap.blocks().get((int) h).hash());
        }
        List<String> applied = incoming.stream().map(Block::hash).toList();

        ChainSnapshot next = store.commit(snap, forkHeight, incoming)
                .orElseThrow(() -> new IllegalStateException(
                        "快照 CAS 提交失败：锁内不应发生，请检查存储实现"));

        SyncStatus status = forkHeight <= before ? SyncStatus.REORGANIZED : SyncStatus.ADVANCED;
        attempts.set(slot, new SourceAttempt(src.name(), true, null,
                "head=" + head + ", forkHeight=" + forkHeight));

        if (status == SyncStatus.REORGANIZED) {
            log.info("链重组已提交：数据源={}，水位 {} -> {}，回滚高度区间=[{}, {}]，"
                            + "回滚哈希={}，重放哈希={}",
                    src.name(), before, next.watermark(), forkHeight, before, rolledBack, applied);
        } else {
            log.info("同步推进已提交：数据源={}，水位 {} -> {}，新增哈希={}",
                    src.name(), before, next.watermark(), applied);
        }

        String msg = status == SyncStatus.REORGANIZED
                ? "在高度 " + forkHeight + " 分叉，已回滚 " + rolledBack.size()
                + " 个区块并重放 " + applied.size() + " 个区块"
                : "新增 " + applied.size() + " 个区块";
        return new SyncResult(status, before, next.watermark(),
                status == SyncStatus.REORGANIZED ? forkHeight : -1,
                List.copyOf(rolledBack), List.copyOf(applied),
                List.copyOf(attempts), msg);
    }

    /**
     * 从本地水位沿高度向下与源比对哈希，返回首个不一致高度（即分叉后第一块的高度）。
     * 若全部一致（纯追加），返回 before + 1；创世即分叉返回 0。
     */
    private long locateForkHeight(BlockSource src, ChainSnapshot snap, long before)
            throws BlockSourceException {
        for (long h = before; h >= 0; h--) {
            Block sourceBlock = requireBlock(src, h);
            Block localBlock = snap.blocks().get((int) h);
            if (!sourceBlock.hash().equals(localBlock.hash())) {
                continue;
            }
            return h + 1;
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
