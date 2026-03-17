package org.apache.rocketmq.hasync.core;

/**
 * SyncSink 工厂接口 — 支持通过工厂方法注入不同的 Sink 实现
 * <p>
 * 对应需求 2 §6（扩展性设计）：无需修改 HASource 或 SyncPipeline 的代码，
 * 即可替换 Sink 实现（如 RocketMQSink、KafkaSink、ElasticsearchSink 等）。
 *
 * @see SyncSink
 */
public interface SyncSinkFactory {

    /**
     * 创建一个 SyncSink 实例
     *
     * @return 新的 SyncSink 实例
     */
    SyncSink createSink();
}
