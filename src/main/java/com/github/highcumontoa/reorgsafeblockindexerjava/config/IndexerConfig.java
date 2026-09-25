package com.github.highcumontoa.reorgsafeblockindexerjava.config;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;
import com.github.highcumontoa.reorgsafeblockindexerjava.service.BlockIndexer;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.BlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.source.LocalBlockSource;
import com.github.highcumontoa.reorgsafeblockindexerjava.store.IndexStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 装配本地数据源与索引服务。
 *
 * <p>两个数据源均为进程内的 {@link LocalBlockSource}，无任何外部网络或真实凭据依赖：
 * <ul>
 *   <li>{@code local-primary}：预置一条 3 个区块的链，作为默认主源；</li>
 *   <li>{@code local-backup}：初始为空链的备用源，用于演示“源落后/不可用时不污染状态”。</li>
 * </ul>
 */
@Configuration
public class IndexerConfig {

    @Bean
    public IndexStore indexStore() {
        return new IndexStore();
    }

    @Bean
    public LocalBlockSource primarySource() {
        LocalBlockSource source = new LocalBlockSource("local-primary");
        source.replaceChain(buildDemoChain(3));
        return source;
    }

    @Bean
    public LocalBlockSource backupSource() {
        return new LocalBlockSource("local-backup");
    }

    @Bean
    public BlockIndexer blockIndexer(IndexStore store) {
        // 注意：不要依赖 Spring 对同类型集合参数的注入顺序（按 bean 名字排序），
        // 这里显式按“主源优先、备源兜底”的顺序构造。
        LocalBlockSource primary = primarySource();
        LocalBlockSource backup = backupSource();
        List<BlockSource> sources = List.of(primary, backup);
        return new BlockIndexer(sources, store);
    }

    /** 构造一条创世链：genesis -> a -> b ...，哈希与父哈希自洽。 */
    static List<Block> buildDemoChain(int n) {
        List<Block> chain = new ArrayList<>();
        String parent = "GENESIS_PARENT";
        for (long h = 0; h < n; h++) {
            String hash = "gen-chain-h" + h;
            chain.add(new Block(h, hash, parent, "demo-payload-" + h));
            parent = hash;
        }
        return chain;
    }
}
