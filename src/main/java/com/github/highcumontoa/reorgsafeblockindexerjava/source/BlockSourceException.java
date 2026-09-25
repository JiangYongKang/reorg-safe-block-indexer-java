package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源异常基类，携带来源名称，便于定位故障来源。
 */
public abstract class BlockSourceException extends RuntimeException {

    private final String sourceName;

    protected BlockSourceException(String sourceName, String message, Throwable cause) {
        super(message, cause);
        this.sourceName = sourceName;
    }

    public String sourceName() {
        return sourceName;
    }
}
