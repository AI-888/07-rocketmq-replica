package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink 写入自动重试策略
 * <p>
 * 对应需求 15 §1-4：
 * <ul>
 *   <li>可重试异常：RemotingException、MQBrokerException、InterruptedException</li>
 *   <li>不可重试异常：消息体超限、Topic 不存在且创建失败</li>
 *   <li>指数退避：100ms, 200ms, 400ms, ... 最大 5s</li>
 *   <li>重试均失败 → syncFailureCount++</li>
 * </ul>
 */
public class SinkRetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(SinkRetryPolicy.class);

    /** 初始重试间隔 */
    private static final long INITIAL_RETRY_INTERVAL = 100;
    /** 最大重试间隔 */
    private static final long MAX_RETRY_INTERVAL = 5000;

    private final int maxRetry;
    private MetricsCollector metricsCollector;

    /** 实际的写入执行器 */
    private WriteExecutor writeExecutor;

    /**
     * 写入执行器接口
     */
    public interface WriteExecutor {
        /**
         * 执行消息写入
         *
         * @param topic   Topic
         * @param body    消息体
         * @param queueId 队列 ID
         * @param properties 消息属性
         * @return 发送结果标识
         * @throws RetryableException 可重试异常
         * @throws NonRetryableException 不可重试异常
         */
        String execute(String topic, byte[] body, int queueId,
                       java.util.Map<String, String> properties) throws Exception;
    }

    /**
     * 可重试异常
     */
    public static class RetryableException extends Exception {
        public RetryableException(String message) {
            super(message);
        }

        public RetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * 不可重试异常
     */
    public static class NonRetryableException extends Exception {
        public NonRetryableException(String message) {
            super(message);
        }

        public NonRetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public SinkRetryPolicy(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    public void setWriteExecutor(WriteExecutor writeExecutor) {
        this.writeExecutor = writeExecutor;
    }

    /**
     * 带重试的写入
     *
     * @return 发送结果标识
     * @throws NonRetryableException 不可重试异常直接抛出
     * @throws RetryableException    重试均失败后抛出最后的异常
     */
    public String sendWithRetry(String topic, byte[] body, int queueId,
                                java.util.Map<String, String> properties) throws Exception {
        if (writeExecutor == null) {
            throw new IllegalStateException("WriteExecutor 未设置");
        }

        Exception lastException = null;
        long retryInterval = INITIAL_RETRY_INTERVAL;

        for (int i = 0; i <= maxRetry; i++) {
            try {
                if (i > 0) {
                    Thread.sleep(retryInterval);
                    retryInterval = Math.min(retryInterval * 2, MAX_RETRY_INTERVAL);
                    if (metricsCollector != null) {
                        metricsCollector.incrementRetryCount();
                    }
                    log.debug("Sink 重试 {}/{}, 等待 {}ms", i, maxRetry, retryInterval / 2);
                }
                return writeExecutor.execute(topic, body, queueId, properties);
            } catch (NonRetryableException e) {
                // 不可重试异常，立即抛出
                log.error("Sink 遇到不可重试异常: {}", e.getMessage());
                throw e;
            } catch (RetryableException e) {
                lastException = e;
                log.warn("Sink 写入失败（第 {} 次）: {}", i + 1, e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                // 其他异常视为可重试
                lastException = e;
                log.warn("Sink 写入异常（第 {} 次）: {}", i + 1, e.getMessage());
            }
        }

        // 重试均失败
        if (metricsCollector != null) {
            metricsCollector.incrementSyncFailureCount();
            metricsCollector.incrementStorageWriteErrorCount();
        }
        log.error("Sink 写入失败，已达最大重试次数 {}", maxRetry);

        throw new RetryableException("写入失败，已重试 " + maxRetry + " 次",
                lastException);
    }

    public int getMaxRetry() {
        return maxRetry;
    }
}
