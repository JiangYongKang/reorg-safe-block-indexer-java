package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.domain.ChainState;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexerService;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.FailureType;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.SourceRouter;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.InMemoryBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceCapabilities;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.InMemoryIndexStore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 链重组场景：分叉后回滚失效区块、重新应用新链，
 * 查询始终一致；日志打印同步水位与回滚范围。
 */
class ChainReorgTest {

    private static final Logger log = LoggerFactory.getLogger(ChainReorgTest.class);

    private InMemoryBlockSource primary;
    private BlockIndexerService indexer;

    private void setUpWithChain(List<Block> chain) {
        primary = new InMemoryBlockSource(
                new SourceCapabilities("local-primary", Long.MAX_VALUE, true, 10));
        primary.setChain(chain);
        InMemoryIndexStore store = new InMemoryIndexStore();
        indexer = new BlockIndexerService(store, new SourceRouter(List.of(primary)));
    }

    /** 快照结构不变量：区块连续、父哈希闭合、tip 信息一致。 */
    private void assertSnapshotIntegrity(ChainState s) {
        List<Block> blocks = s.blocks();
        for (int i = 0; i < blocks.size(); i++) {
            Block b = blocks.get(i);
            assertThat(b.height()).isEqualTo(i);
            if (i == 0) {
                assertThat(b.parentHash()).isNull();
            } else {
                assertThat(b.parentHash()).isEqualTo(blocks.get(i - 1).hash());
            }
        }
        if (blocks.isEmpty()) {
            assertThat(s.tipHeight()).isEqualTo(-1);
        } else {
            assertThat(s.tipHash()).isEqualTo(blocks.get(blocks.size() - 1).hash());
        }
    }

    @Test
    void rollsBackForkedBlocksAndAppliesNewChain() {
        List<Block> chainA = TestChains.chain("A", 5); // 高度 0..4
        setUpWithChain(chainA);

        SyncResult first = indexer.syncOnce();
        assertThat(first.success()).isTrue();
        log.info("[reorg] initial sync watermark={}", first.watermark().height());

        // 在高度 2 处分叉，新链同样延伸到高度 4（区块 2,3,4 被替换）
        List<Block> forked = TestChains.reorg(chainA, "A", 2, 4);
        primary.setChain(forked);

        SyncResult reorg = indexer.syncOnce();
        assertThat(reorg.success()).isTrue();
        assertThat(reorg.reorgRollbackFromHeight()).isEqualTo(2);
        assertThat(reorg.appliedFromHeight()).isEqualTo(2);
        assertThat(reorg.appliedToHeight()).isEqualTo(4);

        ChainState state = indexer.snapshot();
        assertSnapshotIntegrity(state);
        // 共有前缀保持不变
        assertThat(state.blockAt(0).orElseThrow().hash()).isEqualTo("A-h0");
        assertThat(state.blockAt(1).orElseThrow().hash()).isEqualTo("A-h1");
        // 失效区块哈希已从规范链消失
        assertThat(state.blockAt(2).orElseThrow().hash()).contains("fork7");
        assertThat(state.findByHash("A-h2")).isEmpty();
        assertThat(state.findByHash("A-h3")).isEmpty();
        assertThat(state.findByHash("A-h4")).isEmpty();
        assertThat(state.tipHash()).contains("fork7-h4");
        // 水位高度未倒退
        assertThat(state.tipHeight()).isEqualTo(4);
        log.info("[reorg] rollback range=[2,4], applied=[2,4], watermark={}@{}",
                state.tipHeight(), state.tipHash());
    }

    @Test
    void deepReorgReplacesTip() {
        List<Block> chainA = TestChains.chain("A", 5);
        setUpWithChain(chainA);
        indexer.syncOnce();

        // 高度 1 处深度分叉，新链更长（延伸到 6）
        List<Block> deep = TestChains.reorg(chainA, "A", 1, 6);
        primary.setChain(deep);

        SyncResult result = indexer.syncOnce();
        assertThat(result.success()).isTrue();
        assertThat(result.reorgRollbackFromHeight()).isEqualTo(1);
        assertThat(result.appliedToHeight()).isEqualTo(6);

        ChainState state = indexer.snapshot();
        assertSnapshotIntegrity(state);
        assertThat(state.tipHeight()).isEqualTo(6);
        assertThat(state.blockAt(0).orElseThrow().hash()).isEqualTo("A-h0");
        assertThat(state.blockAt(1).orElseThrow().hash()).contains("fork7-h1");
        assertThat(state.blocks()).hasSize(7);
        log.info("[reorg] deep rollback=[1,4], applied=[1,6], watermark={}@{}",
                state.tipHeight(), state.tipHash());
    }

    @Test
    void incompatibleChainWithoutCommonAncestorIsRejected() {
        List<Block> chainA = TestChains.chain("A", 3);
        setUpWithChain(chainA);
        indexer.syncOnce();

        // 连创世区块都不同的链：没有共同祖先，必须拒绝且保持原状态
        List<Block> alien = TestChains.reorg(chainA, "Z", 0, 3);
        primary.setChain(alien);

        SyncResult result = indexer.syncOnce();
        assertThat(result.success()).isFalse();
        assertThat(result.failureType()).isEqualTo(FailureType.BUSINESS);
        assertThat(indexer.snapshot().tipHash()).isEqualTo("A-h2");
        log.info("[reorg] rejected incompatible chain: {}, watermark stays {}",
                result.failureMessage(), indexer.watermark().height());
    }
}
