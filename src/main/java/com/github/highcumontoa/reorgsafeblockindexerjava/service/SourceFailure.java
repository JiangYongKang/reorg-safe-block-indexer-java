package com.github.highcumontoa.reorgsafeblockindexerjava.service;

/**
 * 单个数据源的尝试失败记录，用于可解释结论与日志。
 */
public record SourceFailure(String sourceName, FailureType type, String message) {
}
