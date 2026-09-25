package com.github.highcumontoa.reorgsafeblockindexerjava.store;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.ChainSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 链索引存储。
 *
 * <p>一致性模型：
 * <ul>
 *   <li><b>快照隔离读</b>：内部仅持有一个 {@link AtomicReference} 指向不可变 {@link ChainSnapshot}，
 *       读操作无锁，同步进行中读到的永远是上一个完整提交的快照，绝不会看到回滚一半/写入一半的状态。</li>
 *   <li><b>原子提交</b>：所有回滚与重放先在私有草稿列表上完成，再通过 CAS 整体替换引用。</li>
 *   <li><b>水位单调</b>：提交接口要求新区块必须接在回滚点之后、高度连续，且提交后的水位
 *       不低于提交前（短于本地链的替代链会被服务层拒绝，不会进入本层）。</li>
 * </ul>
 */
public class IndexStore {

    private final AtomicReference<ChainSnapshot> ref = new AtomicReference<>(ChainSnapshot.empty());

    public IndexStore() {
    }

    /** 读取当前已提交快照（同步进行中也可安全调用）。 */
    public ChainSnapshot snapshot() {
        return ref.get();
    }

    public Optional<Block> blockAtHeight(long height) {
        ChainSnapshot s = ref.get();
        if (height < 0 || height >= s.blocks().size()) {
            return Optional.empty();
        }
        return Optional.of(s.blocks().get((int) height));
    }

    public Optional<Block> blockByHash(String hash) {
        ChainSnapshot s = ref.get();
        for (Block b : s.blocks()) {
            if (b.hash().equals(hash)) {
                return Optional.of(b);
            }
        }
        return Optional.empty();
    }

    /** 当前已提交水位（链头高度；空库为 -1）。 */
    public long watermark() {
        return ref.get().watermark();
    }

    /**
     * 在私有草稿上执行“回滚到 forkHeight（不含）+ 顺序应用 newBlocks”，再 CAS 提交。
     *
     * @param expect     调用方读取时所依据的快照；CAS 期间被抢先则提交失败
     * @param forkHeight 共同祖先的高度 + 1，即保留 [0, forkHeight)，回滚 [forkHeight, oldTip]
     * @param newBlocks  从 forkHeight 起按高度连续的新区块
     * @return 提交后的新快照；若 CAS 失败（期间已有其它提交）返回 empty
     * @throws IllegalArgumentException 新区块不连续、不接在回滚点之后或导致水位倒退
     */
    public Optional<ChainSnapshot> commit(ChainSnapshot expect, long forkHeight, List<Block> newBlocks) {
        List<Block> oldBlocks = expect.blocks();
        if (forkHeight < 0 || forkHeight > oldBlocks.size()) {
            throw new IllegalArgumentException("非法回滚点: " + forkHeight
                    + "（当前链长度 " + oldBlocks.size() + "）");
        }

        List<Block> draft = new ArrayList<>(oldBlocks.subList(0, (int) forkHeight));
        long expectedHeight = forkHeight;
        // forkHeight == 0 时没有本地父块，首个区块即创世块，不校验父哈希。
        String expectedParent = forkHeight == 0 ? null : draft.get((int) forkHeight - 1).hash();

        for (Block b : newBlocks) {
            if (b.height() != expectedHeight) {
                throw new IllegalArgumentException("新区块高度不连续: 期望 "
                        + expectedHeight + " 实际 " + b.height());
            }
            if (expectedParent != null && !b.parentHash().equals(expectedParent)) {
                throw new IllegalArgumentException("新区块未接在回滚点之后: 高度 " + b.height()
                        + " 的父哈希应为 " + expectedParent + " 实际为 " + b.parentHash());
            }
            draft.add(b);
            expectedParent = b.hash();
            expectedHeight++;
        }

        // 水位单调：空 newBlocks 且回滚点低于旧水位属于纯回滚，拒绝。
        long newWatermark = draft.size() - 1L;
        if (newWatermark < expect.watermark()) {
            throw new IllegalArgumentException("提交将导致水位倒退: "
                    + expect.watermark() + " -> " + newWatermark);
        }

        ChainSnapshot next = new ChainSnapshot(newWatermark, List.copyOf(draft));
        if (ref.compareAndSet(expect, next)) {
            return Optional.of(next);
        }
        return Optional.empty();
    }
}
