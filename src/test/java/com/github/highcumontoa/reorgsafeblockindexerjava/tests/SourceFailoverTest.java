package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SourceAttempt;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncResult;
import com.github.highcumontoa.reorgsafeblockindexerjava.model.SyncStatus;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTimeoutException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTransportException;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 数据源故障切换场景：
 * 多源按序尝试；超时、传输故障、业务错误必须可区分并归因到具体来源；
 * 源落后/数据不一致不得污染本地状态；任何失败都不得留下部分写入。
 */
class SourceFailoverTest {

    private static final Logger log = LoggerFactory.getLogger(SourceFailoverTest.class);

    @Test
    void failsOverFromTimeoutAndTransportToHealthySource() {
        LocalBlockSource slow = new LocalBlockSource("slow-source");
        LocalBlockSource broken = new LocalBlockSource("broken-source");
        LocalBlockSource healthy = new LocalBlockSource("healthy-source");

        slow.setFailure(new SourceTimeoutException("模拟：节点响应超过 2000ms"));
        broken.setFailure(new SourceTransportException("模拟：连接被对端重置"));
        List<Block> chain = TestChains.chain("H", 4, "ROOT");
        healthy.replaceChain(chain);

        BlockIndexer indexer = new BlockIndexer(
                List.of(slow, broken, healthy), new IndexStore());

        SyncResult result = indexer.sync();
        log.info("[failover] 故障切换结果：状态={}，水位 {} -> {}，逐源结论={}",
                result.status(), result.watermarkBefore(), result.watermarkAfter(),
                result.attempts());

        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(result.watermarkAfter()).isEqualTo(3);
        assertThat(result.attempts()).hasSize(3);

        SourceAttempt timeoutAttempt = result.attempts().get(0);
        SourceAttempt transportAttempt = result.attempts().get(1);
        SourceAttempt okAttempt = result.attempts().get(2);
        assertThat(timeoutAttempt.failureKind()).isEqualTo("TIMEOUT");
        assertThat(transportAttempt.failureKind()).isEqualTo("TRANSPORT");
        assertThat(okAttempt.success()).isTrue();

        assertThat(indexer.snapshot().blocks()).containsExactlyElementsOf(chain);
    }

    @Test
    void allFailureKindsAreReportedAndNothingIsWritten() {
        LocalBlockSource timeout = new LocalBlockSource("t-source");
        LocalBlockSource transport = new LocalBlockSource("tr-source");
        LocalBlockSource business = new LocalBlockSource("b-source");
        timeout.setFailure(new SourceTimeoutException("超时"));
        transport.setFailure(new SourceTransportException("断连"));
        business.setFailure(new SourceBusinessException("区块校验和不匹配"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(
                List.of(timeout, transport, business), store);

        SyncResult result = indexer.sync();
        log.info("[failover] 全部源失败：状态={}，水位={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());

        assertThat(result.status()).isEqualTo(SyncStatus.ALL_SOURCES_UNAVAILABLE);
        assertThat(result.watermarkAfter()).isEqualTo(-1);
        assertThat(result.attempts()).extracting(SourceAttempt::failureKind)
                .containsExactly("TIMEOUT", "TRANSPORT", "BUSINESS");
        assertThat(store.snapshot().blocks()).isEmpty();
    }

    @Test
    void laggingSourceDoesNotPolluteStateAndFailoverContinues() {
        // 主源可达但高度落后（能力不足，属于业务错误），切换到更高的健康源
        LocalBlockSource lagging = new LocalBlockSource("lagging-source");
        LocalBlockSource ahead = new LocalBlockSource("ahead-source");
        lagging.replaceChain(TestChains.chain("L", 2, "ROOT"));
        ahead.replaceChain(TestChains.chain("A", 5, "ROOT"));

        BlockIndexer indexer = new BlockIndexer(
                List.of(lagging, ahead), new IndexStore());
        SyncResult result = indexer.sync();

        log.info("[failover] 落后源切换：状态={}，水位={}，逐源结论={}",
                result.status(), result.watermarkAfter(), result.attempts());
        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(result.watermarkAfter()).isEqualTo(4);
        assertThat(result.attempts()).hasSize(2);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("BUSINESS");
        assertThat(result.attempts().get(0).detail()).contains("落后");
        assertThat(indexer.snapshot().blocks().get(0).hash()).startsWith("A-");
    }

    @Test
    void businessErrorMidFetchLeavesNoPartialWrite() {
        // 源在 headHeight 报告 5，但取高度 3 的区块时抛业务错误：
        // 已拉到的 0..2 必须全部丢弃，本地状态保持空。
        LocalBlockSource flaky = new LocalBlockSource("flaky-source") {
            @Override
            public java.util.Optional<Block> blockAtHeight(long height)
                    throws com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSourceException {
                if (height == 3) {
                    throw new SourceBusinessException("模拟：高度 3 区块内容损坏");
                }
                return super.blockAtHeight(height);
            }
        };
        flaky.replaceChain(TestChains.chain("F", 6, "ROOT"));

        LocalBlockSource healthy = new LocalBlockSource("backup-source");
        healthy.replaceChain(TestChains.chain("F", 6, "ROOT"));

        IndexStore store = new IndexStore();
        BlockIndexer indexer = new BlockIndexer(List.of(flaky, healthy), store);
        SyncResult result = indexer.sync();

        log.info("[failover] 中途业务错误：状态={}，水位={}，故障来源={}，成功来源={}",
                result.status(), result.watermarkAfter(),
                result.attempts().get(0).sourceName() + "/" + result.attempts().get(0).failureKind(),
                result.attempts().get(1).sourceName());

        assertThat(result.status()).isEqualTo(SyncStatus.ADVANCED);
        assertThat(result.attempts().get(0).failureKind()).isEqualTo("BUSINESS");
        assertThat(result.attempts().get(1).success()).isTrue();
        // 最终只有一条完整规范链，没有任何部分前缀残留
        assertThat(store.snapshot().blocks()).hasSize(6);
        assertThat(store.watermark()).isEqualTo(5);
    }
}
