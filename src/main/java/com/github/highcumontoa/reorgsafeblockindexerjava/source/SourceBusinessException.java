package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 业务错误：数据源可达但返回非法/不被接受的数据（如区块缺失、结构非法）。
 */
public class SourceBusinessException extends BlockSourceException {

    public SourceBusinessException(String message) {
        super(message);
    }

    public SourceBusinessException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String kind() {
        return "BUSINESS";
    }
}
