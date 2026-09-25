package com.github.highcumontoa.reorgsafeblockindexerjava.domain;

/**
 * 同步水位：已确认写入规范链的最高高度及其尖端哈希。
 * <p>水位必须单调递增：任何同步失败或回滚中间态都不会推进水位。
 */
public record SyncWatermark(long height, String tipHash, long updatedAtMillis) {

    public static final SyncWatermark EMPTY = new SyncWatermark(-1, null, 0L);
}
