package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.ChainSnapshot;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 等长链切换场景：本地已索引到某高度后，源在同一高度（分叉点甚至更低）给出另一条链。
 * 必须按一次链切换处理：回滚失效段、顺序重放新规范链、水位不变；
 * 切换期间对外只见完整老链或完整新链；并发与失败场景下不产生部分写入。
 */
class ChainSwitchTest {

    private static final Logger log = LoggerFactory.getLogger(ChainSwitchTest.class);

    @Test
    void switchAtGenesisReplacesWholeChainWithoutWatermarkChange() {
        // 分叉点在创世：整条链被替换，水位保持
        LocalBlockSource source = new LocalBlockSource("genesis-switch-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        source.replaceChain(chainA);
        indexer.sync();

        List<Block> chainZ = TestChains.chain("Z", 4, "ROOT");
        source.replaceChain(chainZ);

        SyncResult result = indexer.sync();
        log.info("[switch] 创世分叉切换：状态={}，回滚自高度={}，回滚哈希={}，重放哈希={}，水位={}",
                result.status(), result.rollbackFromHeight(),
                result.rolledBackHashes(), result.appliedHashes(), result.watermarkAfter());

        assertThat(result.status()).isEqualTo(SyncStatus.CHAIN_SWITCHED);
        assertThat(result.rollbackFromHeight()).isEqualTo(0);
        assertThat(result.rolledBackHashes()).containsExactly("A-h0", "A-h1", "A-h2", "A-h3");
        assertThat(result.appliedHashes()).containsExactly("Z-h0", "Z-h1", "Z-h2", "Z-h3");
        assertThat(result.watermarkAfter()).isEqualTo(3);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainZ);
        assertThat(store.blockByHash("A-h0")).isEmpty();
        assertThat(store.blockByHash("Z-h0")).isPresent();
    }

    @Test
    void readersSeeOldOrNewCompleteChainDuringSwitch() throws Exception {
        LocalBlockSource source = new LocalBlockSource("gated-switch-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        source.replaceChain(chainA);
        indexer.sync();

        // 源给出等长分叉链 B（高度 2 处分叉），并让切换在取数阶段阻塞
        List<Block> chainB = TestChains.forkAfter(chainA, 2, "B");
        source.replaceChain(chainB);
        CountDownLatch enteredGate = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        source.setGate(gate);
        source.setOnAwaitGate(enteredGate::countDown);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<SyncResult> future = pool.submit(() -> indexer.sync());
        assertThat(enteredGate.await(5, TimeUnit.SECONDS)).isTrue();

        // 切换进行中：读到的必须是完整的老链 A（水位 4，5 个区块）
        for (int i = 0; i < 20; i++) {
            ChainSnapshot s = indexer.snapshot();
            assertThat(s.watermark()).isEqualTo(4);
            assertThat(s.blocks()).containsExactlyElementsOf(chainA);
        }
        log.info("[switch] 切换进行中读取稳定：完整老链，水位=4");

        gate.countDown();
        SyncResult result = future.get(10, TimeUnit.SECONDS);
        pool.shutdown();
        log.info("[switch] 切换完成：状态={}，回滚自高度={}，水位={}",
                result.status(), result.rollbackFromHeight(), result.watermarkAfter());

        assertThat(result.status()).isEqualTo(SyncStatus.CHAIN_SWITCHED);
        // 切换完成后：读到的是完整的新链 B，绝无半截状态
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainB);
        assertThat(indexer.watermark()).isEqualTo(4);
    }

    @Test
    void concurrentTriggersDuringSwitchOnlyOneCommits() throws Exception {
        LocalBlockSource source = new LocalBlockSource("concurrent-switch-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        source.replaceChain(chainA);
        indexer.sync();

        List<Block> chainB = TestChains.forkAfter(chainA, 1, "B");
        source.replaceChain(chainB);
        CountDownLatch enteredGate = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        source.setGate(gate);
        source.setOnAwaitGate(enteredGate::countDown);

        ExecutorService pool = Executors.newFixedThreadPool(5);
        Future<SyncResult> first = pool.submit(() -> indexer.sync());
        assertThat(enteredGate.await(5, TimeUnit.SECONDS)).isTrue();

        AtomicInteger skipped = new AtomicInteger();
        List<Future<SyncResult>> others = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++) {
            others.add(pool.submit(() -> {
                SyncResult r = indexer.sync();
                if (r.status() == SyncStatus.BUSY_SKIPPED) {
                    skipped.incrementAndGet();
                }
                return r;
            }));
        }
        for (Future<SyncResult> f : others) {
            assertThat(f.get(10, TimeUnit.SECONDS).status()).isEqualTo(SyncStatus.BUSY_SKIPPED);
        }

        gate.countDown();
        SyncResult firstResult = first.get(10, TimeUnit.SECONDS);
        pool.shutdown();
        log.info("[switch] 并发触发：真正切换={}，BUSY_SKIPPED={}，水位={}",
                firstResult.status(), skipped.get(), indexer.watermark());

        assertThat(firstResult.status()).isEqualTo(SyncStatus.CHAIN_SWITCHED);
        assertThat(skipped.get()).isEqualTo(4);
        // 只切换了一次：新链完整、无重复、水位未倒退
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainB);
        assertThat(indexer.watermark()).isEqualTo(3);

        // 切换后重复触发幂等
        SyncResult again = indexer.sync();
        assertThat(again.status()).isEqualTo(SyncStatus.ALREADY_SYNCED);
        assertThat(again.watermarkAfter()).isEqualTo(3);
    }

    @Test
    void midFetchFailureDuringSwitchLeavesOldChainIntactAndFailsOver() {
        // 主源在切换取数中途损坏：本地必须保持完整老链；备源持有同一条新链，切换经备源完成
        LocalBlockSource flaky = new LocalBlockSource("flaky-switch-source") {
            @Override
            public java.util.Optional<Block> blockAtHeight(long height)
                    throws com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSourceException {
                if (height == 3) {
                    throw new SourceBusinessException("模拟：切换取数中途高度 3 数据损坏");
                }
                return super.blockAtHeight(height);
            }
        };
        LocalBlockSource backup = new LocalBlockSource("backup-switch-source");

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(flaky, backup), store);

        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        flaky.replaceChain(chainA);
        backup.replaceChain(chainA);
        indexer.sync();

        List<Block> chainB = TestChains.forkAfter(chainA, 2, "B");
        flaky.replaceChain(chainB);
        backup.replaceChain(chainB);

        SyncResult result = indexer.sync();
        log.info("[switch] 中途失败切换：状态={}，故障源={}/{}，成功源={}，水位={}",
                result.status(),
                result.attempts().get(0).sourceName(), result.attempts().get(0).failureKind(),
                result.attempts().get(1).sourceName(), result.watermarkAfter());

        assertThat(result.status()).isEqualTo(SyncStatus.CHAIN_SWITCHED);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("BUSINESS");
        assertThat(result.attempts().get(1).success()).isTrue();
        // 要么整段成功、要么一个字都不写：最终是完整新链，无任何部分残留
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainB);
        assertThat(indexer.watermark()).isEqualTo(4);
    }

    @Test
    void failedSwitchWithoutBackupKeepsOldChainAndWatermark() {
        // 唯一源在切换取数中途失败：老链完整保留、水位不动，可重试
        java.util.concurrent.atomic.AtomicBoolean corrupt = new java.util.concurrent.atomic.AtomicBoolean(false);
        LocalBlockSource flaky = new LocalBlockSource("flaky-only-source") {
            @Override
            public java.util.Optional<Block> blockAtHeight(long height)
                    throws com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSourceException {
                if (corrupt.get() && height == 2) {
                    throw new SourceBusinessException("模拟：高度 2 数据损坏");
                }
                return super.blockAtHeight(height);
            }
        };
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(flaky), store);

        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        flaky.replaceChain(chainA);
        indexer.sync();

        flaky.replaceChain(TestChains.forkAfter(chainA, 1, "B"));
        corrupt.set(true);
        SyncResult failed = indexer.sync();
        log.info("[switch] 切换失败：状态={}，水位={}", failed.status(), failed.watermarkAfter());

        assertThat(failed.status()).isEqualTo(SyncStatus.ALL_SOURCES_UNAVAILABLE);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainA);
        assertThat(indexer.watermark()).isEqualTo(3);
    }

    @Test
    void chainContinuesToAdvanceAfterSwitch() {
        // 切换完成后源继续延长：正常推进，水位单调不减
        LocalBlockSource source = new LocalBlockSource("advance-after-switch-source");
        BlockIndexer indexer = new BlockIndexer(List.of(source), new IndexStore());

        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        source.replaceChain(chainA);
        indexer.sync();

        List<Block> chainB = TestChains.forkAfter(chainA, 2, "B");
        source.replaceChain(chainB);
        SyncResult switched = indexer.sync();
        assertThat(switched.status()).isEqualTo(SyncStatus.CHAIN_SWITCHED);
        assertThat(switched.watermarkAfter()).isEqualTo(3);

        List<Block> chainBExt = TestChains.extend(chainB, 2, "B-ext");
        source.replaceChain(chainBExt);
        SyncResult advanced = indexer.sync();
        log.info("[switch] 切换后继续推进：状态={}，水位 {} -> {}",
                advanced.status(), advanced.watermarkBefore(), advanced.watermarkAfter());

        assertThat(advanced.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(advanced.watermarkAfter()).isEqualTo(5);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainBExt);
    }
}
