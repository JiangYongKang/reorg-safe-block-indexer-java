package com.github.highcumontoa.reorgsafeblockindexerjava.web;

import java.util.List;

import com.github.highcumontoa.reorgsafeblockindexerjava.service.SourceFailure;

/**
 * REST 错误响应体，携带可区分的失败类型与各数据源尝试记录。
 */
public record ApiError(String error, String message, List<SourceFailure> sources) {
}
