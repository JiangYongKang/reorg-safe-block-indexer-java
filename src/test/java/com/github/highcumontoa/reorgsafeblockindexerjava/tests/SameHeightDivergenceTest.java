package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSourceException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTimeoutException;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * “高度一样、但内容对不上”场景：
 * 多个来源同高度给出不同链时必须识别为来源分歧，本地保持不动；
 * 等来源重新一致后再推进。
 */
class SameHeightDivergenceTest {

    private static final Logger log = LoggerFactory.getLogger(SameHeightDivergenceTest.class);

    @Test
    void equalHeightButDifferentChainsIsDivergenceAndLocalUntouched() {
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        // 两源高度相同（0..4），但高度 3 起是两条不同的链
        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        List<Block> chainB = TestChains.extend(
                TestChains.forkAfter(chainA, 3, "B"), 0, "B");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(chainB);

        SyncResult result = indexer.sync();
        log.info("[diverge] 同高分歧结论：状态={}，水位 {} -> {}，逐源结论={}",
                result.status(), result.watermarkBefore(), result.watermarkAfter(),
                result.attempts());

        assertThat(result.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(result.watermarkAfter()).isEqualTo(-1);
        assertThat(result.attempts()).hasSize(2);
        assertThat(result.attempts()).extracting(a -> a.failureKind())
                .containsOnly("DIVERGENCE");
        // 结论里能看出首个分歧高度
        assertThat(result.message()).contains("高度 3");
        // 没有任何区块被写入本地
        assertThat(store.snapshot().blocks()).isEmpty();
        assertThat(store.watermark()).isEqualTo(-1);
    }

    @Test
    void divergenceKeepsLocalChainThenAdvancesAfterSourcesReconcile() {
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        // 先让两源一致，本地索引到高度 3
        List<Block> common = TestChains.chain("A", 4, "ROOT");
        sourceA.replaceChain(common);
        sourceB.replaceChain(common);
        indexer.sync();
        assertThat(indexer.watermark()).isEqualTo(3);

        // 两源在同一水位 3 给出不同的链（src-B 从高度 2 分叉），本地必须原样不动
        List<Block> forkB = TestChains.forkAfter(common, 2, "B");
        sourceB.replaceChain(forkB);

        SyncResult diverged = indexer.sync();
        log.info("[diverge] 已索引后同高分歧：状态={}，水位保持={}，本地链仍是A",
                diverged.status(), diverged.watermarkAfter());
        assertThat(diverged.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(diverged.watermarkAfter()).isEqualTo(3);
        assertThat(diverged.message()).contains("高度 2");
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(common);
        assertThat(store.blockAtHeight(2).orElseThrow().hash()).isEqualTo("A-h2");

        // 来源重新对得上：src-B 切回 A 并共同延长到高度 5，同步继续推进
        List<Block> reconciled = TestChains.extend(common, 2, "A-ext");
        sourceA.replaceChain(reconciled);
        sourceB.replaceChain(reconciled);

        SyncResult advanced = indexer.sync();
        log.info("[diverge] 来源重新一致后推进：状态={}，水位 {} -> {}，新增哈希={}",
                advanced.status(), advanced.watermarkBefore(), advanced.watermarkAfter(),
                advanced.appliedHashes());
        assertThat(advanced.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(advanced.watermarkAfter()).isEqualTo(5);
        assertThat(advanced.appliedHashes()).containsExactly("A-ext-h4", "A-ext-h5");
        assertThat(advanced.attempts()).extracting(a -> a.success())
                .containsExactly(true, true);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(reconciled);
    }

    @Test
    void agreeingSourcesAtSameWatermarkTriggerAtomicChainSwitch() {
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        // 本地先索引链 A 到高度 4
        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(chainA);
        indexer.sync();

        // 两个来源一致地在同一水位 4 给出另一条链 B（高度 1 起就分叉，属于“更低位置”分叉）
        List<Block> chainB = TestChains.forkAfter(chainA, 1, "B");
        sourceA.replaceChain(chainB);
        sourceB.replaceChain(chainB);

        SyncResult switched = indexer.sync();
        log.info("[diverge] 多源一致的同水位链切换：状态={}，水位 {} -> {}，回滚区间起点={}，"
                        + "回滚哈希={}，重放哈希={}",
                switched.status(), switched.watermarkBefore(), switched.watermarkAfter(),
                switched.rollbackFromHeight(), switched.rolledBackHashes(),
                switched.appliedHashes());

        assertThat(switched.status()).isEqualTo(SyncStatus.REORGANIZED);
        assertThat(switched.watermarkBefore()).isEqualTo(4);
        assertThat(switched.watermarkAfter()).isEqualTo(4); // 等长：水位不变
        assertThat(switched.rollbackFromHeight()).isEqualTo(1);
        assertThat(switched.rolledBackHashes()).containsExactly(
                "A-h1", "A-h2", "A-h3", "A-h4");
        assertThat(switched.appliedHashes()).containsExactly(
                "B-h1", "B-h2", "B-h3", "B-h4");
        // 两个来源都标记成功（共识一致）
        assertThat(switched.attempts()).hasSize(2);
        assertThat(switched.attempts()).allSatisfy(a -> assertThat(a.success()).isTrue());

        // 本地已是完整新链，查询不到任何旧链分叉段
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainB);
        assertThat(store.blockAtHeight(0).orElseThrow().hash()).isEqualTo("A-h0");
        assertThat(store.blockAtHeight(1).orElseThrow().hash()).isEqualTo("B-h1");
        assertThat(store.blockByHash("A-h1")).isEmpty();
    }

    @Test
    void repeatedSameDivergenceGivesStableConclusionAndNeverWrites() {
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        // 本地先有已提交链
        List<Block> chainA = TestChains.chain("A", 4, "ROOT");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(chainA);
        indexer.sync();

        // 同样的分歧反复出现 5 次
        List<Block> forkB = TestChains.forkAfter(chainA, 2, "B");
        sourceB.replaceChain(forkB);

        SyncResult first = null;
        for (int i = 0; i < 5; i++) {
            SyncResult r = indexer.sync();
            assertThat(r.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
            assertThat(r.watermarkAfter()).isEqualTo(3); // 水位绝不后退
            assertThat(r.rolledBackHashes()).isEmpty();   // 没有任何回滚
            assertThat(r.appliedHashes()).isEmpty();      // 没有任何写入
            if (first == null) {
                first = r;
            } else {
                // 结论稳定：首个分歧高度、逐源 failureKind、消息完全一致
                assertThat(r.message()).isEqualTo(first.message());
                assertThat(r.attempts()).usingRecursiveFieldByFieldElementComparator()
                        .containsExactlyElementsOf(first.attempts());
            }
        }
        log.info("[diverge] 重复 5 次同高分歧结论稳定：水位={}，消息={}",
                indexer.watermark(), first.message());

        // 本地链自始至终未动
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chainA);

        // 来源恢复一致（共同延长），下一次同步立刻推进，服务没有被卡住
        List<Block> reconciled = TestChains.extend(chainA, 1, "A-ext");
        sourceB.replaceChain(reconciled);
        sourceA.replaceChain(reconciled);
        SyncResult advanced = indexer.sync();
        log.info("[diverge] 分歧恢复后推进：状态={}，水位 {} -> {}",
                advanced.status(), advanced.watermarkBefore(), advanced.watermarkAfter());
        assertThat(advanced.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(advanced.watermarkAfter()).isEqualTo(4);
    }

    @Test
    void concurrentDivergenceSyncsRunAtMostOneEvaluation() throws Exception {
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        List<Block> chainB = TestChains.forkAfter(chainA, 3, "B");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(chainB);

        int threads = 6;

        // 让取数确定性地阻塞在途中，制造“同步进行中”窗口。
        // 被 tryLock 拒绝的线程不访问数据源、直接返回；真正那次同步会在源上阻塞。
        java.util.concurrent.atomic.AtomicInteger arrivals =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch gate =
                new java.util.concurrent.CountDownLatch(1);
        sourceA.setGate(gate);
        sourceA.setOnAwaitGate(() -> arrivals.incrementAndGet());

        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch start =
                new java.util.concurrent.CountDownLatch(1);
        java.util.List<java.util.concurrent.Future<SyncResult>> futures =
                new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return indexer.sync();
            }));
        }

        start.countDown();
        // 等到至少一次取数在源上阻塞、且其余 threads-1 个触发都已结束（即都得到
        // BUSY_SKIPPED），再放行门闩——保证结论确定：恰好 1 次真正评估。
        org.awaitility.Awaitility.await().atMost(5, java.util.concurrent.TimeUnit.SECONDS)
                .until(() -> arrivals.get() >= 1
                        && futures.stream().filter(java.util.concurrent.Future::isDone).count()
                        == threads - 1);
        // 在真正那次同步仍在途时，本地水位保持初始值
        assertThat(indexer.watermark()).isEqualTo(-1);
        gate.countDown();

        int diverged = 0;
        int busy = 0;
        for (java.util.concurrent.Future<SyncResult> f : futures) {
            SyncResult r = f.get(10, java.util.concurrent.TimeUnit.SECONDS);
            if (r.status() == SyncStatus.SOURCES_DIVERGED) {
                diverged++;
            } else if (r.status() == SyncStatus.BUSY_SKIPPED) {
                busy++;
            } else {
                throw new AssertionError("意外状态: " + r.status());
            }
        }
        pool.shutdown();

        log.info("[diverge] 分歧下并发同步 {} 次：真正评估(SOURCES_DIVERGED)={}，"
                        + "BUSY_SKIPPED={}，水位={}", threads, diverged, busy, indexer.watermark());
        assertThat(diverged).isEqualTo(1);
        assertThat(busy).isEqualTo(threads - 1);
        assertThat(indexer.watermark()).isEqualTo(-1);
        assertThat(store.snapshot().blocks()).isEmpty();
    }

    @Test
    void midFetchFailureFallsBackToHealthySourceWithNoPartialWrite() {
        // 坏源在取高度 3 时超时；健康源给出同一条链：单独成组即共识，正常推进，
        // 已从坏源拉到的 0..2 随草稿丢弃，本地不留半截前缀。
        LocalBlockSource flaky = new LocalBlockSource("flaky-source") {
            @Override
            public java.util.Optional<Block> blockAtHeight(long height)
                    throws BlockSourceException {
                if (height == 3) {
                    throw new SourceTimeoutException("模拟：取高度 3 超时");
                }
                return super.blockAtHeight(height);
            }
        };
        LocalBlockSource healthy = new LocalBlockSource("healthy-source");
        List<Block> chain = TestChains.chain("M", 6, "ROOT");
        flaky.replaceChain(chain);
        healthy.replaceChain(chain);

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(flaky, healthy), store);

        SyncResult result = indexer.sync();
        log.info("[diverge] 取数中途超时后采用健康源：状态={}，水位={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());
        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(result.watermarkAfter()).isEqualTo(5);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("TIMEOUT");
        assertThat(result.attempts().get(1).success()).isTrue();
        assertThat(store.snapshot().blocks()).containsExactlyElementsOf(chain);
    }

    @Test
    void divergenceWithThirdSourceFailingStillReportedAndNothingWritten() {
        // src-A 与 src-B 同高度但内容不同；src-C 取数中途损坏。
        // 仍然必须判定来源分歧（而不是把单个健康侧当共识），本地不动。
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        LocalBlockSource sourceC = new LocalBlockSource("src-C") {
            @Override
            public java.util.Optional<Block> blockAtHeight(long height)
                    throws BlockSourceException {
                if (height == 2) {
                    throw new SourceBusinessException("模拟：高度 2 数据损坏");
                }
                return super.blockAtHeight(height);
            }
        };
        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        List<Block> chainB = TestChains.forkAfter(chainA, 3, "B");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(chainB);
        sourceC.replaceChain(chainA);

        IndexStore store = new IndexStore();
        BlockIndexer indexer =
                new BlockIndexer(List.of(sourceA, sourceB, sourceC), store);

        SyncResult result = indexer.sync();
        log.info("[diverge] 分歧+第三源中途损坏：状态={}，逐源结论={}",
                result.status(), result.attempts());
        assertThat(result.status()).isEqualTo(SyncStatus.SOURCES_DIVERGED);
        assertThat(result.message()).contains("高度 3");
        assertThat(result.attempts()).hasSize(3);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("DIVERGENCE");
        assertThat(result.attempts().get(1).failureKind()).isEqualTo("DIVERGENCE");
        assertThat(result.attempts().get(2).failureKind()).isEqualTo("BUSINESS");
        assertThat(store.snapshot().blocks()).isEmpty();
    }

    @Test
    void sameTipButDifferentInteriorIsDetectedByFullHeightComparison() {
        // 构造一条“tip 哈希与链 A 相同、但高度 2 内容不同”的畸形链（父哈希因此不自洽）。
        // 全长逐高度比对必须识别出深处分歧，不能只看 tip 就当成同一条链。
        List<Block> chainA = TestChains.chain("A", 5, "ROOT");
        List<Block> tampered = new java.util.ArrayList<>(chainA);
        tampered.set(2, new Block(2, "TAMPERED-h2", "A-h1", "tampered"));

        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        sourceA.replaceChain(chainA);
        sourceB.replaceChain(tampered);

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);

        SyncResult result = indexer.sync();
        log.info("[diverge] tip相同深处不同：状态={}，逐源结论={}",
                result.status(), result.attempts());

        // src-B 链不自洽（h3 的父哈希对不上）会被整链校验判为 BUSINESS；
        // 只剩 src-A 一个来源时以它为共识正常推进——绝不把畸形链误判为一致。
        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(result.watermarkAfter()).isEqualTo(4);
        assertThat(result.attempts().get(0).success()).isTrue();
        assertThat(result.attempts().get(1).failureKind()).isEqualTo("BUSINESS");
        assertThat(result.attempts().get(1).detail()).contains("不自洽");
        assertThat(store.snapshot().blocks()).containsExactlyElementsOf(chainA);
    }

    @Test
    void strictlyShorterConsensusIsRejectedAndWatermarkNeverRegresses() {
        // 本地已到水位 5；两个来源一致地只到高度 3（严格更短）。
        // 等长才允许切换；严格变短会导致水位倒退，必须拒绝，本地保持完整老链。
        LocalBlockSource sourceA = new LocalBlockSource("src-A");
        LocalBlockSource sourceB = new LocalBlockSource("src-B");
        List<Block> longer = TestChains.chain("A", 6, "ROOT");
        List<Block> shorter = TestChains.chain("A", 4, "ROOT");
        sourceA.replaceChain(longer);
        sourceB.replaceChain(longer);

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(sourceA, sourceB), store);
        indexer.sync();
        assertThat(indexer.watermark()).isEqualTo(5);

        // 两源一起回退到更短的高度
        sourceA.replaceChain(shorter);
        sourceB.replaceChain(shorter);

        SyncResult result = indexer.sync();
        log.info("[diverge] 共识链严格更短：状态={}，水位保持={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());
        assertThat(result.status()).isEqualTo(SyncStatus.ALL_SOURCES_UNAVAILABLE);
        assertThat(result.watermarkAfter()).isEqualTo(5);
        assertThat(result.attempts()).hasSize(2);
        assertThat(result.attempts()).allSatisfy(a -> {
            assertThat(a.success()).isFalse();
            assertThat(a.failureKind()).isEqualTo("BUSINESS");
            assertThat(a.detail()).contains("落后");
        });
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(longer);
    }
}
