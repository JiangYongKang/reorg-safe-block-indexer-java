package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
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
 * 数据源故障切换：超时/传输/业务错误可区分，
 * 主源故障自动切换备源，全部失败时给出可解释结论且无部分写入。
 */
class SourceFailoverTest {

    private static final Logger log = LoggerFactory.getLogger(SourceFailoverTest.class);

    private InMemoryBlockSource primary;
    private InMemoryBlockSource secondary;
    private InMemoryIndexStore store;
    private BlockIndexerService indexer;

    private void setUp(List<Block> primaryChain, List<Block> secondaryChain) {
        primary = new InMemoryBlockSource(
                new SourceCapabilities("local-primary", Long.MAX_VALUE, true, 10));
        secondary = new InMemoryBlockSource(
                new SourceCapabilities("local-secondary", Long.MAX_VALUE, true, 20));
        primary.setChain(primaryChain);
        secondary.setChain(secondaryChain);
        store = new InMemoryIndexStore();
        indexer = new BlockIndexerService(store,
                new SourceRouter(List.of(secondary, primary)));
    }

    @Test
    void timeoutFailsOverToSecondary() {
        List<Block> chain = TestChains.chain("A", 4);
        setUp(chain, chain);
        // 主源头永久不可用，迫使路由切到备源
        primary.failHeadOnceTimeout();
        primary.failOnceTimeout(0);
        primary.failOnceTimeout(1);
        primary.failOnceTimeout(2);
        primary.failOnceTimeout(3);

        SyncResult r = indexer.syncOnce();
        assertThat(r.success()).isTrue();
        assertThat(r.sourceName()).isEqualTo("local-secondary");
        assertThat(store.snapshot().blocks()).hasSize(4);
        log.info("[failover] TIMEOUT on local-primary -> switched to {}, watermark={}",
                r.sourceName(), r.watermark().height());
    }

    @Test
    void transportFailureFailsOverToSecondary() {
        List<Block> chain = TestChains.chain("A", 3);
        setUp(chain, chain);
        primary.failHeadOnceTransport();
        for (long h = 0; h < 3; h++) {
            primary.failOnceTransport(h);
        }

        SyncResult r = indexer.syncOnce();
        assertThat(r.success()).isTrue();
        assertThat(r.sourceName()).isEqualTo("local-secondary");
        log.info("[failover] TRANSPORT on local-primary -> switched to {}, watermark={}",
                r.sourceName(), r.watermark().height());
    }

    @Test
    void allSourcesFailWithExplainableConclusion() {
        List<Block> chain = TestChains.chain("A", 3);
        setUp(chain, chain);
        // 两个源都超时
        primary.failHeadOnceTimeout();
        secondary.failHeadOnceTimeout();

        SyncResult timeoutAll = indexer.syncOnce();
        assertThat(timeoutAll.success()).isFalse();
        assertThat(timeoutAll.failureType()).isEqualTo(FailureType.TIMEOUT);
        assertThat(timeoutAll.attemptedSources()).hasSize(2);
        assertThat(timeoutAll.attemptedSources())
                .extracting(s -> s.sourceName())
                .containsExactly("local-primary", "local-secondary");
        assertThat(store.snapshot().blocks()).isEmpty();
        log.warn("[failover] all sources TIMEOUT: {}; attempted={}",
                timeoutAll.failureMessage(), timeoutAll.attemptedSources());

        // 主源超时、备源传输 -> 主导类别取业务无关规则下的多数（此处 1:1 -> TRANSPORT 优先于 TIMEOUT）
        primary.failHeadOnceTimeout();
        secondary.failHeadOnceTransport();
        SyncResult mixed = indexer.syncOnce();
        assertThat(mixed.success()).isFalse();
        assertThat(mixed.failureType()).isEqualTo(FailureType.TRANSPORT);
        assertThat(store.snapshot().blocks()).isEmpty();
        log.warn("[failover] mixed failure conclusion={} sources={}",
                mixed.failureType(), mixed.attemptedSources());

        // 恢复后成功，水位从零建立，证明失败没有留下部分写入
        SyncResult recovered = indexer.syncOnce();
        assertThat(recovered.success()).isTrue();
        assertThat(store.snapshot().blocks()).hasSize(3);
        log.info("[failover] recovered, watermark={}", store.watermark().height());
    }

    @Test
    void divergentSourcesDoNotPolluteLocalState() {
        // 初始时两个源看到完全一致的链（高度 0..3）
        List<Block> chainA = TestChains.chain("A", 4);
        setUp(chainA, chainA);
        indexer.syncOnce();
        assertThat(store.snapshot().tipHash()).isEqualTo("A-h3");

        // 随后两个源在同一高度 4 上报告不同链头（模拟数据源之间结果不一致）
        List<Block> aLonger = TestChains.chain("A", 5);
        List<Block> bLonger = TestChains.reorg(chainA, "B", 2, 4);
        primary.setChain(aLonger);
        secondary.setChain(bLonger);

        SyncResult r = indexer.syncOnce();
        assertThat(r.success()).isFalse();
        assertThat(r.failureType()).isEqualTo(FailureType.BUSINESS);
        assertThat(r.failureMessage()).contains("diverge");
        // 本地状态完全不被污染：仍停留在 A 链高度 3
        assertThat(store.snapshot().tipHeight()).isEqualTo(3);
        assertThat(store.snapshot().tipHash()).isEqualTo("A-h3");
        log.warn("[failover] divergent sources rejected; failure={}, sources={}, watermark stays {}",
                r.failureType(), r.attemptedSources(), store.watermark().height());
    }

    @Test
    void capabilityLimitedSourceIsSkipped() {
        // 主源能力只到高度 1，备源无限制；同步到高度 4 必须自动选择备源
        primary = new InMemoryBlockSource(
                new SourceCapabilities("local-limited", 1, true, 5));
        secondary = new InMemoryBlockSource(
                new SourceCapabilities("local-secondary", Long.MAX_VALUE, true, 20));
        List<Block> chain = TestChains.chain("A", 5);
        primary.setChain(chain.subList(0, 2));
        secondary.setChain(chain);
        store = new InMemoryIndexStore();
        indexer = new BlockIndexerService(store,
                new SourceRouter(List.of(primary, secondary)));

        SyncResult r = indexer.syncOnce();
        assertThat(r.success()).isTrue();
        assertThat(r.sourceName()).isEqualTo("local-secondary");
        assertThat(store.snapshot().tipHeight()).isEqualTo(4);
        log.info("[failover] limited-capability source skipped, used {}, watermark={}",
                r.sourceName(), r.watermark().height());
    }
}
