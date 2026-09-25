package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源在限定时间内未响应。
 */
public class SourceTimeoutException extends BlockSourceException {

    public SourceTimeoutException(String message) {
        super(message);
    }

    public SourceTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String kind() {
        return "TIMEOUT";
    }
}
