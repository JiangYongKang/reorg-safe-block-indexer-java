package com.github.highcumontoa.reorgsafeblockindexerjava.tests;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexerService;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.SourceRouter;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.InMemoryBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceCapabilities;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.InMemoryIndexStore;
import com.github.highcumontoa.reorgsafeblockindexerjava.web.ChainQueryController;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 接口冒烟测试：直接装配真实服务 + 本地模拟源（standalone MockMvc，无需启动 Web 容器）。
 */
class ChainQueryControllerTest {

    private static final Logger log = LoggerFactory.getLogger(ChainQueryControllerTest.class);

    private MockMvc mvc;
    private InMemoryBlockSource source;

    @BeforeEach
    void setUp() {
        source = new InMemoryBlockSource(
                new SourceCapabilities("local-primary", Long.MAX_VALUE, true, 10));
        source.setChain(TestChains.chain("A", 3));
        InMemoryIndexStore store = new InMemoryIndexStore();
        BlockIndexerService indexer =
                new BlockIndexerService(store, new SourceRouter(List.of(source)));
        mvc = MockMvcBuilders.standaloneSetup(new ChainQueryController(indexer))
                .setMessageConverters(new MappingJackson2HttpMessageConverter())
                .build();
    }

    @Test
    void endpointsExposeConsistentView() throws Exception {
        // 同步前链为空
        mvc.perform(get("/api/chain"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tipHeight").value(-1));

        mvc.perform(post("/api/sync").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.watermark.height").value(2));
        log.info("[api] POST /api/sync advanced watermark to 2");

        // 重复同步幂等
        mvc.perform(post("/api/sync").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));

        mvc.perform(get("/api/blocks/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.hash").value("A-h1"))
                .andExpect(jsonPath("$.height").value(1));

        mvc.perform(get("/api/blocks/hash/A-h0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.parentHash").doesNotExist());

        mvc.perform(get("/api/blocks").param("from", "0").param("to", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3));

        // 重复投递幂等
        mvc.perform(post("/api/blocks")
                        .accept(MediaType.APPLICATION_JSON)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"height\":1,\"hash\":\"A-h1\",\"parentHash\":\"A-h0\","
                                + "\"timestampMillis\":1700000001001}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true));
        log.info("[api] duplicate POST /api/blocks ignored idempotently");

        // 查不存在的高度
        mvc.perform(get("/api/blocks/99"))
                .andExpect(status().isNotFound());
    }

    @Test
    void failedSyncReturnsExplainableError() throws Exception {
        source.failHeadOnceTimeout();
        mvc.perform(post("/api/sync").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.error").value("TIMEOUT"))
                .andExpect(jsonPath("$.sources[0].sourceName").value("local-primary"));
        log.info("[api] failed sync reported TIMEOUT with source detail");
    }
}
