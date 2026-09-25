package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试用链构造工具：便于构造自洽链与分叉链。
 */
final class TestChains {

    private TestChains() {
    }

    /** 构造前缀为 tag 的自洽链，长度 length，创世父哈希为 parentOfGenesis。 */
    static List<Block> chain(String tag, int length, String parentOfGenesis) {
        List<Block> blocks = new ArrayList<>();
        String parent = parentOfGenesis;
        for (long h = 0; h < length; h++) {
            String hash = tag + "-h" + h;
            blocks.add(new Block(h, hash, parent, tag + "-payload-" + h));
            parent = hash;
        }
        return blocks;
    }

    /** 在共享前缀 [0, prefixLen) 之上构造一条分叉：前缀取自 base，分叉块用 forkTag。 */
    static List<Block> forkAfter(List<Block> base, int prefixLen, String forkTag) {
        List<Block> blocks = new ArrayList<>(base.subList(0, prefixLen));
        String parent = prefixLen == 0 ? "FORK_GENESIS_PARENT"
                : base.get(prefixLen - 1).hash();
        for (long h = prefixLen; h < base.size(); h++) {
            String hash = forkTag + "-h" + h;
            blocks.add(new Block(h, hash, parent, forkTag + "-payload-" + h));
            parent = hash;
        }
        return blocks;
    }

    /** 延长链：在 base 之后追加 extra 个区块。 */
    static List<Block> extend(List<Block> base, int extra, String tag) {
        List<Block> blocks = new ArrayList<>(base);
        String parent = base.get(base.size() - 1).hash();
        for (int i = 0; i < extra; i++) {
            long h = blocks.size();
            String hash = tag + "-h" + h;
            blocks.add(new Block(h, hash, parent, tag + "-payload-" + h));
            parent = hash;
        }
        return blocks;
    }
}
