package com.github.highcumontoa.reorgsafeblockindexerjava.domain;

/**
 * 区块数据模型（不可变值对象）。
 *
 * @param height          区块高度（从 0 开始）
 * @param hash            区块哈希（唯一标识）
 * @param parentHash      父区块哈希，创世区块为 null
 * @param timestampMillis 区块时间戳（毫秒）
 */
public record Block(long height, String hash, String parentHash, long timestampMillis) {
}
