package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.InMemoryBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceBusinessException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceCapabilities;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTimeoutException;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceTransportException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryBlockSourceTest {

    private InMemoryBlockSource source(String name) {
        return new InMemoryBlockSource(
                new SourceCapabilities(name, Long.MAX_VALUE, true, 10));
    }

    @Test
    void servesConfiguredChain() {
        InMemoryBlockSource src = source("local-a");
        src.setChain(TestChains.chain("A", 4));

        assertThat(src.head().orElseThrow().hash()).isEqualTo("A-h3");
        assertThat(src.fetchBlock(0).hash()).isEqualTo("A-h0");
        assertThat(src.fetchBlocks(1, 3)).hasSize(3);
        System.out.println("[source] head=" + src.head().orElseThrow().hash());
    }

    @Test
    void injectedFailuresAreClassified() {
        InMemoryBlockSource src = source("local-a");
        src.setChain(TestChains.chain("A", 4));

        src.failOnceTimeout(1);
        assertThatThrownBy(() -> src.fetchBlock(1))
                .isInstanceOf(SourceTimeoutException.class)
                .hasMessageContaining("local-a");
        // one-shot 故障只影响一次，随后恢复
        assertThat(src.fetchBlock(1).hash()).isEqualTo("A-h1");
        System.out.println("[source] TIMEOUT injected then recovered at height 1 from local-a");

        src.failOnceTransport(2);
        assertThatThrownBy(() -> src.fetchBlock(2))
                .isInstanceOf(SourceTransportException.class);
        System.out.println("[source] TRANSPORT injected at height 2 from local-a");

        src.failOnceBusiness(3);
        assertThatThrownBy(() -> src.fetchBlock(3))
                .isInstanceOf(SourceBusinessException.class);
        System.out.println("[source] BUSINESS injected at height 3 from local-a");
    }

    @Test
    void chainSwapSimulatesReorgAtSource() {
        InMemoryBlockSource src = source("local-a");
        src.setChain(TestChains.chain("A", 3));
        List<Block> forked = TestChains.reorg(TestChains.chain("A", 3), "A", 1, 3);
        src.setChain(forked);
        assertThat(src.fetchBlock(1).hash()).contains("fork7");
        System.out.println("[source] chain swapped to fork, new head=" + src.head().orElseThrow().hash());
    }
}
