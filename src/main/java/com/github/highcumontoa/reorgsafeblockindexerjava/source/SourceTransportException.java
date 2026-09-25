package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 传输层故障：连接断开、IO 错误等。
 */
public class SourceTransportException extends BlockSourceException {

    public SourceTransportException(String message) {
        super(message);
    }

    public SourceTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String kind() {
        return "TRANSPORT";
    }
}
