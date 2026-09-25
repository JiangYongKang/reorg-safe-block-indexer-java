package com.github.highcumontoa.reorgsafeblockindexerjava.model;

/**
 * 一次同步/投递尝试的结果状态。
 */
public enum SyncStatus {
    /** 已推进到新的水位 */
    ADVANCED,
    /** 链已重组（回滚并重放） */
    REORGANIZED,
    /** 数据已存在，幂等跳过（重复同步/重复投递） */
    ALREADY_SYNCED,
    /** 已有其它同步在进行，本次触发被跳过 */
    BUSY_SKIPPED,
    /** 投递被拒绝：高度跳跃/无法接到当前链上，未产生任何写入 */
    REJECTED,
    /** 所有数据源均不可用 */
    ALL_SOURCES_UNAVAILABLE
}
