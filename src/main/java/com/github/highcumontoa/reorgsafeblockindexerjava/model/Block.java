package com.github.highcumontoa.reorgsafeblockindexerjava.model;

/**
 * 规范化区块。
 *
 * @param height       区块高度（从 0 开始）
 * @param hash         本区块哈希（唯一标识）
 * @param parentHash   父区块哈希
 * @param payload      业务数据
 */
public record Block(long height, String hash, String parentHash, String payload) {
}
