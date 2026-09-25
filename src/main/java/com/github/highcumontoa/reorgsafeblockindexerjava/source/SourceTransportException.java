package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源传输故障（连接断开等，可切换其他源重试）。
 */
public class SourceTransportException extends BlockSourceException {

    public SourceTransportException(String sourceName, String message, Throwable cause) {
        super(sourceName, message, cause);
    }
}
