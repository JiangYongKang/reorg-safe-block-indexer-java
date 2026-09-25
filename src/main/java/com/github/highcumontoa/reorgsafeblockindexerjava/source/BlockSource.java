package com.github.highcumontoa.reorgsafeblockindexerjava.source;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.Block;

import java.util.List;
import java.util.Optional;

/**
 * 区块数据源抽象。实现只能来自本地进程或仓库已有依赖，禁止访问外部网络。
 */
public interface BlockSource {

    /** 能力声明（名称、最大高度、批量支持、优先级）。 */
    SourceCapabilities capabilities();

    /** 拉取最新链头（空表示数据源当前没有任何区块）。 */
    Optional<Block> head();

    /** 按高度拉取单个区块。 */
    Block fetchBlock(long height);

    /** 批量拉取 [fromHeight, toHeight] 闭区间区块；不支持批量时抛出业务错误。 */
    List<Block> fetchBlocks(long fromHeight, long toHeight);
}
