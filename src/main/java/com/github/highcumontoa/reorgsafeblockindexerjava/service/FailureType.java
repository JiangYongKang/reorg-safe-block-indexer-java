package com.github.highcumontoa.reorgsafeblockindexerjava.service;

/**
 * 可解释的失败类型。
 * <ul>
 *   <li>{@link #TIMEOUT} 所有可用源均超时</li>
 *   <li>{@link #TRANSPORT} 所有可用源均出现传输故障</li>
 *   <li>{@link #BUSINESS} 数据源内容非法/不一致/能力不足（非传输问题）</li>
 *   <li>{@link #NO_SOURCE} 没有任何能力满足需求的数据源</li>
 * </ul>
 */
public enum FailureType {
    TIMEOUT,
    TRANSPORT,
    BUSINESS,
    NO_SOURCE
}
