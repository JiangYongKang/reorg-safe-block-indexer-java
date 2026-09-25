package com.github.highcumontoa.reorgsafeblockindexerjava.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 不可变的规范链视图快照。
 * <p>每次同步成功都会整体替换发布一个新的 ChainState，
 * 因此读取方在任何时刻看到的都是完整一致的链视图，绝不会看到回滚中或写入一半的状态。
 * <p>快照内区块按高度连续排列（0..tipHeight），父哈希链接的校验由索引服务在发布前完成。
 *
 * @param blocks 按高度排序的完整区块列表（高度即列表下标）
 */
public record ChainState(List<Block> blocks) {

    public ChainState {
        List<Block> sorted = new ArrayList<>(blocks == null ? List.of() : blocks);
        sorted.sort((a, b) -> Long.compare(a.height(), b.height()));
        TreeMap<Long, Block> heightMap = new TreeMap<>();
        TreeMap<String, Block> hashMap = new TreeMap<>();
        for (Block b : sorted) {
            if (heightMap.put(b.height(), b) != null || hashMap.put(b.hash(), b) != null) {
                throw new IllegalArgumentException("chain contains duplicate block: " + b);
            }
        }
        blocks = List.copyOf(sorted);
    }

    public static ChainState from(List<Block> chain) {
        return new ChainState(chain);
    }

    public static ChainState empty() {
        return new ChainState(List.of());
    }

    @JsonProperty("tipHeight")
    public long tipHeight() {
        return blocks.isEmpty() ? -1 : blocks.get(blocks.size() - 1).height();
    }

    @JsonProperty("tipHash")
    public String tipHash() {
        return blocks.isEmpty() ? null : blocks.get(blocks.size() - 1).hash();
    }

    /** 按高度查询区块。 */
    public Optional<Block> blockAt(long height) {
        if (height < 0 || height > Integer.MAX_VALUE || height >= blocks.size()) {
            return Optional.empty();
        }
        Block b = blocks.get((int) height);
        return b.height() == height ? Optional.of(b) : Optional.empty();
    }

    /** 按哈希查询区块。 */
    public Optional<Block> findByHash(String hash) {
        if (hash == null) {
            return Optional.empty();
        }
        for (Block b : blocks) {
            if (b.hash().equals(hash)) {
                return Optional.of(b);
            }
        }
        return Optional.empty();
    }

    public boolean containsHash(String hash) {
        return findByHash(hash).isPresent();
    }

    /** 当前水位（快照不可变，时间戳取快照读取时刻）。 */
    public SyncWatermark watermark() {
        if (tipHeight() < 0) {
            return SyncWatermark.EMPTY;
        }
        return new SyncWatermark(tipHeight(), tipHash(), System.currentTimeMillis());
    }
}
