package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源能力描述（高度上限、是否支持批量拉取等）。
 * 能力不满足需求的源不会被选中，避免低能力源污染本地状态。
 */
public record SourceCapabilities(String name, long maxHeight, boolean batchFetchSupported, int priority) {

    /** 本地模拟源的默认优先级（数字越小优先级越高）。 */
    public static final int DEFAULT_PRIORITY = 100;
}
