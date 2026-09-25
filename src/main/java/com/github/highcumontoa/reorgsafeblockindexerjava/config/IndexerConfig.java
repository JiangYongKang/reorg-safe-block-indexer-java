package com.github.highcumontoa.reorgsafeblockindexerjava.config;

import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexerService;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.SourceRouter;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.InMemoryIndexStore;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 装配索引服务。
 * <p>数据源仅使用本地模拟实现（{@code local-primary} / {@code local-secondary}），
 * 无任何外部网络或真实凭据。测试可自行构造实例，不依赖 Spring 容器。
 */
@Configuration
public class IndexerConfig {

    @Bean
    public InMemoryIndexStore indexStore() {
        return new InMemoryIndexStore();
    }

    @Bean
    public SourceRouter sourceRouter(List<BlockSource> sources) {
        return new SourceRouter(sources);
    }

    @Bean
    public BlockIndexerService blockIndexerService(InMemoryIndexStore store, SourceRouter router) {
        return new BlockIndexerService(store, router);
    }
}
