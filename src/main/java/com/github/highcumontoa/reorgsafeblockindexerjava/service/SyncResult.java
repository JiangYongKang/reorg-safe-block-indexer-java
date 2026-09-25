package com.github.highcumontoa.reorgsafeblockindexerjava.service;

import com.github.highcumontoa.reorgsafeblockindexerjava.domain.SyncWatermark;

import java.util.List;

/**
 * 一次同步尝试的结论。
 * <p>{@code success=false} 时 {@code failureType} 必非 null，且 {@code watermark}
 * 保持为同步前的旧水位（水位绝不因失败或回滚倒退）。
 */
public record SyncResult(
        boolean success,
        SyncWatermark previousWatermark,
        SyncWatermark watermark,
        long reorgRollbackFromHeight,
        long appliedFromHeight,
        long appliedToHeight,
        String sourceName,
        FailureType failureType,
        String failureMessage,
        List<SourceFailure> attemptedSources,
        boolean duplicate,
        String detail
) {

    public static SyncResult success(SyncWatermark previous, SyncWatermark current,
                                     long rollbackFrom, long appliedFrom, long appliedTo,
                                     String sourceName, boolean duplicate, String detail) {
        return new SyncResult(true, previous, current, rollbackFrom, appliedFrom, appliedTo,
                sourceName, null, null, List.of(), duplicate, detail);
    }

    public static SyncResult failure(SyncWatermark current, FailureType type, String message,
                                     List<SourceFailure> attempted) {
        return new SyncResult(false, current, current, -1, -1, -1,
                null, type, message, attempted == null ? List.of() : List.copyOf(attempted),
                false, message);
    }
}
