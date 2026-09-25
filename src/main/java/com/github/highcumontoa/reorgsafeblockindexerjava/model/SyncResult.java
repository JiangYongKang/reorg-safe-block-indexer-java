package com.github.highcumontoa.reorgsafeblockindexerjava.model;

import java.util.List;

/**
 * 一次同步尝试的可解释结果。
 */
public record SyncResult(
        SyncStatus status,
        long watermarkBefore,
        long watermarkAfter,
        long rollbackFromHeight,
        List<String> rolledBackHashes,
        List<String> appliedHashes,
        List<SourceAttempt> attempts,
        String message
) {
}
