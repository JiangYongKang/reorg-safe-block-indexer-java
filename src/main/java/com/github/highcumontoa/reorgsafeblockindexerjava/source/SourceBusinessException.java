package com.github.highcumontoa.reorgsafeblockindexerjava.source;

/**
 * 数据源业务错误：数据源可达，但返回内容非法/不一致/不支持。
 * <p>是否可切换由路由器决定（全部源均报业务错误才向调用方返回失败结论）。
 */
public class SourceBusinessException extends BlockSourceException {

    public SourceBusinessException(String sourceName, String message) {
        super(sourceName, message, null);
    }
}
