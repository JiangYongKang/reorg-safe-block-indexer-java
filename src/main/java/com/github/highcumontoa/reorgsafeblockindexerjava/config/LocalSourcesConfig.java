package com.github.highcumontoa.reorgsafeblockindexerjava.config;

import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.InMemoryBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.SourceCapabilities;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 默认装配两个空的本地模拟源（主/备），仅用于应用启动与上下文测试。
 * 功能性测试直接构造源并喂入数据。
 */
@Configuration
public class LocalSourcesConfig {

    @Bean
    public BlockSource localPrimarySource() {
        return new InMemoryBlockSource(new SourceCapabilities("local-primary", Long.MAX_VALUE, true, 10));
    }

    @Bean
    public BlockSource localSecondarySource() {
        return new InMemoryBlockSource(new SourceCapabilities("local-secondary", Long.MAX_VALUE, true, 20));
    }
}
