package com.github.highcumontoa.reorgsafeblockindexerjava.model;

import java.util.List;

/**
 * 某一时刻链索引的不可变快照。任何同步进行中，对外查询永远只能看到已提交的快照。
 */
public final class ChainSnapshot {

    private final long watermark;
    private final List<Block> blocks;

    public ChainSnapshot(long watermark, List<Block> blocks) {
        this.watermark = watermark;
        this.blocks = blocks;
    }

    public long watermark() {
        return watermark;
    }

    public List<Block> blocks() {
        return blocks;
    }

    public static ChainSnapshot empty() {
        return new ChainSnapshot(-1L, List.of());
    }
}
