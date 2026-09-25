package com.github.highcumontoa.reorgsafeblockindexerjava.source;

import com.github.highcumontoa.reorgsafeblockindexerjava.model.Block;

import java.util.Optional;

/**
 * 区块数据源抽象。实现可以是本地模拟链，也可以是任何进程内可访问的来源；
 * 本工程只提供本地实现，不依赖外部网络或真实凭据。
 */
public interface BlockSource {

    /** 数据源名称，用于日志与失败归因 */
    String name();

    /** 该源当前的链头高度；失败时抛出区分类型的异常 */
    long headHeight() throws BlockSourceException;

    /**
     * 按高度取区块；源认为该高度不存在时返回 empty。
     */
    Optional<Block> blockAtHeight(long height) throws BlockSourceException;

    /**
     * 按哈希取区块（用于分叉定位）；不存在时返回 empty。
     */
    Optional<Block> blockByHash(String hash) throws BlockSourceException;
}
