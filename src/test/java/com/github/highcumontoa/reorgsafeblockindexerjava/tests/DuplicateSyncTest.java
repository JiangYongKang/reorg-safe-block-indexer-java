package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.ChainSnapshot;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重复同步 / 重复投递 / 并发触发场景：
 * 不得产生重复数据、不得高度倒退，同步进行中读仍然可用。
 */
class DuplicateSyncTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSyncTest.class);

    @Test
    void repeatedSyncAndDuplicateSubmissionAreIdempotent() {
        LocalBlockSource source = new LocalBlockSource("dup-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        List<Block> chain = TestChains.chain("D", 4, "ROOT");
        source.replaceChain(chain);

        indexer.sync();
        SyncResult second = indexer.sync();
        SyncResult third = indexer.sync();
        log.info("[dup] 连续重复同步：第二次状态={}，第三次状态={}，水位={}",
                second.status(), third.status(), indexer.watermark());
        assertThat(second.status()).isEqualTo(SyncStatus.ALREADY_SYNCED);
        assertThat(third.status()).isEqualTo(SyncStatus.ALREADY_SYNCED);

        // 重复投递已存在的区块
        Block duplicate = chain.get(2);
        SyncResult dup = indexer.submitBlock(duplicate);
        log.info("[dup] 重复投递：状态={}，高度={}，水位保持={}",
                dup.status(), duplicate.height(), dup.watermarkAfter());
        assertThat(dup.status()).isEqualTo(SyncStatus.ALREADY_SYNCED);

        // 再次投递同高度但不同哈希的区块（冲突），必须拒绝
        Block conflict = new Block(2, "EVIL-h2", "D-h1", "evil");
        SyncResult rejected = indexer.submitBlock(conflict);
        log.info("[dup] 冲突投递：状态={}，原因={}", rejected.status(), rejected.message());
        assertThat(rejected.status()).isEqualTo(SyncStatus.REJECTED);

        // 最终无重复、无倒退
        assertThat(indexer.snapshot().blocks()).hasSize(4);
        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chain);
        assertThat(store.blockAtHeight(2).orElseThrow().hash()).isEqualTo("D-h2");
    }

    @Test
    void concurrentSyncsDoNotDuplicateOrRegress() throws Exception {
        LocalBlockSource source = new LocalBlockSource("concurrent-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);
        source.replaceChain(TestChains.chain("K", 10, "ROOT"));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger advanced = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        AtomicInteger already = new AtomicInteger();

        List<Future<?>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                SyncResult r = indexer.sync();
                switch (r.status()) {
                    case ADVANCED -> advanced.incrementAndGet();
                    case BUSY_SKIPPED -> skipped.incrementAndGet();
                    case ALREADY_SYNCED -> already.incrementAndGet();
                    default -> throw new AssertionError("意外状态: " + r.status());
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        log.info("[concurrent] 并发同步 {} 次：ADVANCED={}，BUSY_SKIPPED={}，ALREADY_SYNCED={}，最终水位={}",
                threads, advanced.get(), skipped.get(), already.get(), indexer.watermark());

        assertThat(indexer.watermark()).isEqualTo(9);
        assertThat(indexer.snapshot().blocks()).hasSize(10);
        assertThat(advanced.get()).isEqualTo(1);
        assertThat(skipped.get() + already.get()).isEqualTo(threads - 1);
    }

    @Test
    void readsRemainAvailableAndConsistentDuringSync() throws Exception {
        LocalBlockSource source = new LocalBlockSource("gated-source");
        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(source), store);

        // 预置已提交数据
        source.replaceChain(TestChains.chain("R", 3, "ROOT"));
        indexer.sync();
        long watermarkBefore = indexer.watermark();

        // 让下一次同步在取数阶段阻塞，并能确定性地感知“同步已在途”
        CountDownLatch enteredGate = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        source.replaceChain(TestChains.chain("R", 8, "ROOT"));
        source.setGate(gate);
        source.setOnAwaitGate(enteredGate::countDown);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<SyncResult> future = pool.submit(() -> indexer.sync());

        // 等待同步线程确实已在源上阻塞，再开始断言“同步进行中读仍然稳定”
        assertThat(enteredGate.await(5, TimeUnit.SECONDS)).isTrue();

        // 在同步阻塞期间反复读取：必须始终读到旧的完整快照（水位 2，3 个区块）
        for (int i = 0; i < 20; i++) {
            ChainSnapshot s = indexer.snapshot();
            assertThat(s.watermark()).isEqualTo(watermarkBefore);
            assertThat(s.blocks()).hasSize(3);
            Optional<Block> b = store.blockAtHeight(2);
            assertThat(b).isPresent();
        }
        log.info("[read-during-sync] 同步进行中读取稳定：水位={}，区块数=3", watermarkBefore);

        gate.countDown();
        SyncResult result = future.get(10, TimeUnit.SECONDS);
        pool.shutdown();
        log.info("[read-during-sync] 同步结束：状态={}，水位 {} -> {}",
                result.status(), result.watermarkBefore(), result.watermarkAfter());
        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(indexer.watermark()).isEqualTo(7);
        assertThat(indexer.snapshot().blocks()).hasSize(8);
    }
}
