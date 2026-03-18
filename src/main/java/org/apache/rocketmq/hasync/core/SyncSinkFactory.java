package org.apache.rocketmq.hasync.core;

/**
 * SyncSink 工厂接口 — 支持通过工厂方法注入不同的 Sink 实现
 * <p>
 * 对应需求 2 §6（扩展性设计）：无需修改 HASource 或 SourceBootstrap 的代码，
 * 即可替换 Sink 实现（如 RocketMQSink、KafkaSink、ElasticsearchSink 等）。
 * <p>
 * 所有 Sink 实现统一通过 ZMQ REQ Socket 从 Source 拉取数据，
 * 无论是独立部署还是 --with-sink 同进程内嵌模式。
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
