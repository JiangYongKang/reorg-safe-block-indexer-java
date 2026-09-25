package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源超时（可切换其他源重试）。
 */
public class SourceTimeoutException extends BlockSourceException {

    public SourceTimeoutException(String sourceName, String message, Throwable cause) {
        super(sourceName, message, cause);
    }
}
