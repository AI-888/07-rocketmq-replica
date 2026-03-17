package org.apache.rocketmq.hasync.model;

/**
 * ZMQ Pull 响应状态枚举
 */
public enum ResponseStatus {

    /** 成功返回数据 */
    SUCCESS,

    /** 暂无新消息 */
    NO_NEW_MSG,

    /** 偏移量非法（如请求的偏移量已过期） */
    OFFSET_ILLEGAL,

    /** 内部错误 */
    ERROR
}
