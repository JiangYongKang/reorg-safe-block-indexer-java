package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SourceAttempt;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
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
 * 源间分歧场景：多个可用源在同一高度给出互相矛盾的链时，
 * 不挑选任何一方、本地保持原样、结论可读且稳定；源恢复一致后继续推进。
 */
class SourceDivergenceTest {

    private static final Logger log = LoggerFactory.getLogger(SourceDivergenceTest.class);

    @Test
    void divergentSourcesAreDetectedAndNothingIsWritten() {
        // 两个源都可用、高度相同（0..3），但一条是链 X、一条是链 Y
        LocalBlockSource sourceX = new LocalBlockSource("source-x");
        LocalBlockSource sourceY = new LocalBlockSource("source-y");
        List<Block> chainX = TestChains.chain("X", 4, "ROOT");
        List<Block> chainY = TestChains.chain("Y", 4, "ROOT");
        sourceX.replaceChain(chainX);
        sourceY.replaceChain(chainY);

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceX, sourceY), store);

        SyncResult result = indexer.sync();
        log.info("[divergence] 两源分歧：状态={}，水位={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());

        assertThat(result.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(result.watermarkAfter()).isEqualTo(-1);
        assertThat(result.rolledBackHashes()).isEmpty();
        assertThat(result.appliedHashes()).isEmpty();
        // 结论可区分：每个源都标明自己看到的 tip 与对立阵营
        assertThat(result.attempts()).hasSize(2);
        assertThat(result.attempts()).extracting(SourceAttempt::failureKind)
                .containsExactly("DIVERGENT", "DIVERGENT");
        assertThat(result.attempts().get(0).detail()).contains("X-h3");
        assertThat(result.attempts().get(1).detail()).contains("Y-h3");
        assertThat(result.message()).contains("X-h3").contains("Y-h3");
        // 本地保持原样：一个字都没写
        assertThat(store.snapshot().blocks()).isEmpty();
        assertThat(store.watermark()).isEqualTo(-1);
    }

    @Test
    void majorityDoesNotWinWhenSourcesDisagree() {
        // 三个源：两个持有链 X、一个持有链 Y。即使 2:1，也不允许“随便挑多数派”
        LocalBlockSource x1 = new LocalBlockSource("x1");
        LocalBlockSource x2 = new LocalBlockSource("x2");
        LocalBlockSource y1 = new LocalBlockSource("y1");
        List<Block> chainX = TestChains.chain("X", 3, "ROOT");
        x1.replaceChain(chainX);
        x2.replaceChain(chainX);
        y1.replaceChain(TestChains.chain("Y", 3, "ROOT"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(x1, x2, y1), store);

        SyncResult result = indexer.sync();
        log.info("[divergence] 2:1 分歧：状态={}，水位={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());

        assertThat(result.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(result.attempts()).extracting(SourceAttempt::failureKind)
                .containsExactly("DIVERGENT", "DIVERGENT", "DIVERGENT");
        assertThat(store.snapshot().blocks()).isEmpty();
    }

    @Test
    void syncResumesAfterSourcesAgreeAgain() {
        LocalBlockSource sourceX = new LocalBlockSource("source-x");
        LocalBlockSource sourceY = new LocalBlockSource("source-y");
        List<Block> chainX = TestChains.chain("X", 4, "ROOT");
        sourceX.replaceChain(chainX);
        sourceY.replaceChain(TestChains.chain("Y", 4, "ROOT"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceX, sourceY), store);

        SyncResult diverged = indexer.sync();
        assertThat(diverged.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        log.info("[divergence] 分歧期间：状态={}，水位={}", diverged.status(), diverged.watermarkAfter());

        // 源 Y 重新与 X 对齐后，同步立即恢复推进
        sourceY.replaceChain(chainX);
        SyncResult resumed = indexer.sync();
        log.info("[divergence] 恢复一致后：状态={}，水位 {} -> {}",
                resumed.status(), resumed.watermarkBefore(), resumed.watermarkAfter());

        assertThat(resumed.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(resumed.watermarkAfter()).isEqualTo(3);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainX);
    }

    @Test
    void repeatedDivergenceGivesStableConclusionWithoutWrites() {
        LocalBlockSource sourceX = new LocalBlockSource("source-x");
        LocalBlockSource sourceY = new LocalBlockSource("source-y");
        sourceX.replaceChain(TestChains.chain("X", 5, "ROOT"));
        sourceY.replaceChain(TestChains.chain("Y", 5, "ROOT"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceX, sourceY), store);

        // 同一分歧反复出现：每次结论一致、可区分，水位不动，服务不卡死
        for (int i = 1; i <= 3; i++) {
            SyncResult r = indexer.sync();
            log.info("[divergence] 第 {} 次重复触发：状态={}，水位={}", i, r.status(), r.watermarkAfter());
            assertThat(r.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
            assertThat(r.watermarkAfter()).isEqualTo(-1);
            assertThat(r.attempts()).extracting(SourceAttempt::failureKind)
                    .containsExactly("DIVERGENT", "DIVERGENT");
        }
        assertThat(store.watermark()).isEqualTo(-1);
        assertThat(store.snapshot().blocks()).isEmpty();
    }

    @Test
    void divergenceKeepsPreviouslySyncedChainIntact() {
        // 本地已索引链 A（0..2）之后，两个源在同一高度给出 X/Y 两条不同的新链
        LocalBlockSource sourceX = new LocalBlockSource("source-x");
        LocalBlockSource sourceY = new LocalBlockSource("source-y");
        List<Block> chainA = TestChains.chain("A", 3, "ROOT");
        sourceX.replaceChain(chainA);
        sourceY.replaceChain(chainA);

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceX, sourceY), store);
        indexer.sync();
        assertThat(indexer.watermark()).isEqualTo(2);

        sourceX.replaceChain(TestChains.chain("X", 3, "ROOT"));
        sourceY.replaceChain(TestChains.chain("Y", 3, "ROOT"));

        SyncResult result = indexer.sync();
        log.info("[divergence] 已同步后分歧：状态={}，水位={}", result.status(), result.watermarkAfter());

        assertThat(result.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        // 本地维持切换前完整的老链 A，不被分歧污染
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainA);
        assertThat(indexer.watermark()).isEqualTo(2);
    }

    @Test
    void concurrentSyncsDuringDivergenceOnlyOneEvaluates() throws Exception {
        LocalBlockSource sourceX = new LocalBlockSource("source-x");
        LocalBlockSource sourceY = new LocalBlockSource("source-y");
        sourceX.replaceChain(TestChains.chain("X", 4, "ROOT"));
        sourceY.replaceChain(TestChains.chain("Y", 4, "ROOT"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceX, sourceY), store);

        // 用门闩让第一次同步在取数阶段阻塞，制造确定性的“评估在途”窗口
        CountDownLatch enteredGate = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        sourceX.setGate(gate);
        sourceX.setOnAwaitGate(enteredGate::countDown);
        sourceY.setGate(gate);

        ExecutorService pool = Executors.newFixedThreadPool(5);
        Future<SyncResult> first = pool.submit(() -> indexer.sync());
        assertThat(enteredGate.await(5, TimeUnit.SECONDS)).isTrue();

        // 评估在途期间的并发触发：必须全部被干脆跳过
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
            SyncResult r = f.get(10, TimeUnit.SECONDS);
            log.info("[divergence] 在途期间并发触发：状态={}", r.status());
            assertThat(r.status()).isEqualTo(SyncStatus.BUSY_SKIPPED);
        }

        gate.countDown();
        SyncResult firstResult = first.get(10, TimeUnit.SECONDS);
        pool.shutdown();
        log.info("[divergence] 在途评估完成：状态={}，BUSY_SKIPPED={}，水位={}",
                firstResult.status(), skipped.get(), indexer.watermark());

        // 同一时刻最多一次真正评估分歧；本地始终未写入
        assertThat(firstResult.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(skipped.get()).isEqualTo(4);
        assertThat(store.snapshot().blocks()).isEmpty();
        assertThat(store.watermark()).isEqualTo(-1);

        // 评估结束后再触发：同一分歧给出稳定、相同的结论，服务不卡死
        SyncResult again = indexer.sync();
        assertThat(again.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(store.watermark()).isEqualTo(-1);
    }
}
