package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源失败类型的抽象基类，区分超时、传输故障与业务错误。
 */
public abstract class BlockSourceException extends Exception {

    protected BlockSourceException(String message) {
        super(message);
    }

    protected BlockSourceException(String message, Throwable cause) {
        super(message, cause);
    }

    /** 机器可识别的失败类别 */
    public abstract String kind();
}
