package com.github.highcumontoa.reorgsafeblockindexerjava.model;

/**
 * 单个数据源的一次尝试记录，用于可解释的失败结论。
 */
public record SourceAttempt(
        String sourceName,
        boolean success,
        String failureKind,
        String detail
) {
}
