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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重复同步 / 重复投递 / 并发同步场景：
 * 无重复数据、水位单调、同步进行中仍可读。
 */
class DuplicateSyncTest {

    private static final Logger log = LoggerFactory.getLogger(DuplicateSyncTest.class);

    private InMemoryBlockSource source;
    private InMemoryIndexStore store;
    private BlockIndexerService indexer;

    private void setUp(List<Block> chain) {
        source = new InMemoryBlockSource(
                new SourceCapabilities("local-primary", Long.MAX_VALUE, true, 10));
        source.setChain(chain);
        store = new InMemoryIndexStore();
        indexer = new BlockIndexerService(store, new SourceRouter(List.of(source)));
    }

    @Test
    void repeatedSyncIsIdempotent() {
        setUp(TestChains.chain("A", 4));
        SyncResult first = indexer.syncOnce();
        assertThat(first.success()).isTrue();
        long tipHeight = first.watermark().height();

        for (int i = 0; i < 5; i++) {
            SyncResult r = indexer.syncOnce();
            assertThat(r.success()).isTrue();
            assertThat(r.duplicate()).isTrue();
            assertThat(r.watermark().height()).isEqualTo(tipHeight);
        }
        assertThat(indexer.snapshot().blocks()).hasSize(4);
        log.info("[duplicate] 5 repeated syncs, watermark stays at {}", tipHeight);
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        setUp(TestChains.chain("A", 4));
        indexer.syncOnce();
        Block b2 = source.fetchBlock(2);

        SyncResult once = indexer.ingest(b2);
        SyncResult twice = indexer.ingest(b2);
        SyncResult thrice = indexer.ingest(b2);
        assertThat(once.duplicate()).isTrue();
        assertThat(twice.duplicate()).isTrue();
        assertThat(thrice.duplicate()).isTrue();
        assertThat(indexer.snapshot().blocks()).hasSize(4);
        // 水位不因重复投递变化
        assertThat(indexer.watermark().height()).isEqualTo(3);
        log.info("[duplicate] block {} delivered 3x, watermark stays {}",
                b2.hash(), indexer.watermark().height());
    }

    @Test
    void deliveryOfNextBlockExtendsChain() {
        setUp(TestChains.chain("A", 2));
        indexer.syncOnce(); // tip = 1
        // 模拟消息先于同步到达：投递高度 2 的区块
        source.setChain(TestChains.chain("A", 3));
        Block next = source.fetchBlock(2);
        SyncResult r = indexer.ingest(next);
        assertThat(r.success()).isTrue();
        assertThat(indexer.snapshot().tipHeight()).isEqualTo(2);
        log.info("[duplicate] gap-free delivery extended watermark to {}",
                indexer.watermark().height());
    }

    @Test
    void gappedDeliveryIsRejectedWithoutPartialWrite() {
        setUp(TestChains.chain("A", 2));
        indexer.syncOnce(); // tip = 1
        source.setChain(TestChains.chain("A", 5));
        Block jumping = source.fetchBlock(4);
        SyncResult r = indexer.ingest(jumping);
        assertThat(r.success()).isFalse();
        assertThat(r.failureType()).isEqualTo(FailureType.BUSINESS);
        assertThat(indexer.snapshot().tipHeight()).isEqualTo(1);
        log.info("[duplicate] gapped delivery rejected: {}", r.failureMessage());
    }

    @Test
    void concurrentSyncKeepsSingleConsistentState() throws Exception {
        setUp(TestChains.chain("A", 8));
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        // 同步进行中并发读取：任何时刻快照都必须结构完整
        ExecutorService readers = Executors.newSingleThreadExecutor();
        Future<?> readerTask = readers.submit(() -> {
            long endAt = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < endAt) {
                ChainState s = indexer.snapshot();
                assertSnapshotReadable(s);
            }
        });

        List<Future<SyncResult>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                barrier.await(5, TimeUnit.SECONDS);
                SyncResult r = indexer.syncOnce();
                if (r.success() && !r.duplicate()) {
                    successes.incrementAndGet();
                }
                if (r.duplicate()) {
                    duplicates.incrementAndGet();
                }
                return r;
            }));
        }
        for (Future<SyncResult> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS).success()).isTrue();
        }
        readerTask.get(5, TimeUnit.SECONDS);
        pool.shutdown();
        readers.shutdown();

        // 恰好一次真正应用，其余为重复
        assertThat(successes.get()).isEqualTo(1);
        assertThat(duplicates.get()).isEqualTo(threads - 1);
        assertThat(indexer.snapshot().blocks()).hasSize(8);
        assertThat(indexer.watermark().height()).isEqualTo(7);
        log.info("[duplicate] {} concurrent syncs: 1 applied, {} duplicates, watermark={}",
                threads, duplicates.get(), indexer.watermark().height());
    }

    private void assertSnapshotReadable(ChainState s) {
        List<Block> blocks = s.blocks();
        for (int i = 0; i < blocks.size(); i++) {
            Block b = blocks.get(i);
            assertThat(b.height()).isEqualTo(i);
            if (i > 0) {
                assertThat(b.parentHash()).isEqualTo(blocks.get(i - 1).hash());
            }
        }
    }
}
