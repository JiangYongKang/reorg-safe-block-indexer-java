package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试用链构造工具：生成父子哈希闭合的区块序列，
 * 并支持在指定高度处分叉（替换后缀）。
 */
final class TestChains {

    private TestChains() {
    }

    /** 构造长度为 size（高度 0..size-1）的链，哈希前缀为 prefix。 */
    static List<Block> chain(String prefix, int size) {
        return chainFrom(prefix, 0, null, size, 0);
    }

    /**
     * 从给定父区块延续一条链。
     *
     * @param prefix   哈希前缀
     * @param startH   起始高度
     * @param parent   父区块（startH 处区块的父哈希来源），null 表示从创世开始
     * @param count    生成区块数量（含 startH）
     * @param forkSalt 分叉盐：非 0 时生成不同哈希
     */
    static List<Block> chainFrom(String prefix, long startH, Block parent, int count, int forkSalt) {
        List<Block> out = new ArrayList<>();
        String parentHash = parent == null ? null : parent.hash();
        long expectedPrevHeight = parent == null ? -1 : parent.height();
        for (int i = 0; i < count; i++) {
            long height = startH + i;
            if (height > 0 && parent == null && i == 0) {
                throw new IllegalArgumentException("genesis must start at height 0");
            }
            String hash = hashOf(prefix, height, forkSalt);
            out.add(new Block(height, hash, parentHash, 1_700_000_000_000L + height));
            parentHash = hash;
        }
        if (parent != null && expectedPrevHeight + 1 != startH) {
            throw new IllegalArgumentException("chain does not extend parent contiguously");
        }
        return out;
    }

    /**
     * 在 base 链的 forkHeight 处分叉：保留 [0, forkHeight) 共有前缀，
     * 用新盐生成 forkHeight..tipHeight 的分叉后缀。
     */
    static List<Block> reorg(List<Block> base, String prefix, int forkHeight, int newTipHeight) {
        List<Block> out = new ArrayList<>(base.subList(0, forkHeight));
        Block parent = forkHeight == 0 ? null : base.get(forkHeight - 1);
        int count = newTipHeight - forkHeight + 1;
        out.addAll(chainFrom(prefix, forkHeight, parent, count, 7));
        return out;
    }

    static String hashOf(String prefix, long height, int forkSalt) {
        if (forkSalt == 0) {
            return prefix + "-h" + height;
        }
        return prefix + "-fork" + forkSalt + "-h" + height;
    }
}
