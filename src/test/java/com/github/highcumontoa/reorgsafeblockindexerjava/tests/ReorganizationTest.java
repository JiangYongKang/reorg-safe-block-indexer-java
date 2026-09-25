package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 链重组场景：源先给出链 A，随后在高度 3 处分叉为更长的链 B，
 * 索引必须回滚失效区块、重放新链，且查询永远只看到完整提交的状态。
 */
class ReorganizationTest {

    private static final Logger log = LoggerFactory.getLogger(ReorganizationTest.class);

    @Test
    void rollsBackStaleBlocksAndAppliesCanonicalChain() {
        LocalBlockSource source = new LocalBlockSource("reorg-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        // 1) 初始链 A：高度 0..4
        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        source.replaceChain(chainA);

        SyncResult first = indexer.sync();
        log.info("[reorg] 初次同步：水位 {} -> {}，状态={}",
                first.watermarkBefore(), first.watermarkAfter(), first.status());
        assertThat(first.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(first.watermarkAfter()).isEqualTo(4);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainA);

        // 2) 源在高度 3 处分叉，且新链更长（0..5）
        List<Block> chainB = TestChains.extend(
                TestChains.forkAfter(chainA, 3, "B"), 1, "B-ext");
        source.replaceChain(chainB);

        SyncResult reorg = indexer.sync();
        log.info("[reorg] 重组同步：水位 {} -> {}，回滚区间=[{}, {}]，回滚哈希={}，重放哈希={}",
                reorg.watermarkBefore(), reorg.watermarkAfter(),
                reorg.rollbackFromHeight(), reorg.watermarkBefore(),
                reorg.rolledBackHashes(), reorg.appliedHashes());

        assertThat(reorg.status()).isEqualTo(SyncStatus.REORGANIZED);
        assertThat(reorg.rollbackFromHeight()).isEqualTo(3);
        assertThat(reorg.rolledBackHashes()).containsExactly("A-h3", "A-h4");
        assertThat(reorg.appliedHashes()).containsExactly("B-h3", "B-h4", "B-ext-h5");
        assertThat(reorg.watermarkAfter()).isEqualTo(5);

        // 3) 最终规范化视图完全等于链 B，旧哈希不可再按高度查到
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainB);
        assertThat(store.blockAtHeight(3).orElseThrow().hash()).isEqualTo("B-h3");
        assertThat(store.blockByHash("A-h3")).isEmpty();
        assertThat(store.blockByHash("B-h3")).isPresent();

        // 4) 再次同步幂等，水位不回退
        SyncResult again = indexer.sync();
        log.info("[reorg] 重复同步：状态={}，水位={}", again.status(), again.watermarkAfter());
        assertThat(again.status()).isEqualTo(SyncStatus.ALREADY_SYNCED);
        assertThat(again.watermarkAfter()).isEqualTo(5);
    }

    @Test
    void equalLengthForkIsNotApplied() {
        // 等长分叉：水位不允许倒退，必须拒绝且给出 BUSINESS 归因
        LocalBlockSource source = new LocalBlockSource("equal-fork-source");
        BlockIndexer indexer = new BlockIndexer(List.of(source), new IndexStore());

        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        source.replaceChain(chainA);
        indexer.sync();

        List<Block> fork = TestChains.forkAfter(chainA, 2, "C");
        source.replaceChain(fork);

        SyncResult result = indexer.sync();
        log.info("[reorg] 等长分叉结论：状态={}，尝试明细={}",
                result.status(), result.attempts());
        assertThat(result.status()).isEqualTo(SyncStatus.ALL_SOURCES_UNAVAILABLE);
        assertThat(result.attempts()).hasSize(1);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("BUSINESS");
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainA);
    }
}
