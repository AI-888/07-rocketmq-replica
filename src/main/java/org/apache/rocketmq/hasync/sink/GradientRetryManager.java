package org.apache.rocketmq.hasync.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * 梯度退避重试管理器（需求 21 §21.3）
 * <p>
 * 写入失败时按梯度时间间隔重试（默认 1s → 3s → 10s → 30s → 60s），
 * 所有重试耗尽后暂停同步任务（SUSPENDED），等待人工通过 POST /resume 恢复。
 * <p>
 * 线程安全，支持并发调用。
 */
public class GradientRetryManager {

    private static final Logger log = LoggerFactory.getLogger(GradientRetryManager.class);

    /** 默认梯度重试等待时间（秒） */
    public static final int[] DEFAULT_RETRY_DELAYS = {1, 3, 10, 30, 60};

    /** 默认最大重试次数 */
    public static final int DEFAULT_MAX_RETRY = 5;

    private final int maxRetry;
    private final int[] retryDelays;
    private final AtomicInteger currentRetryLevel = new AtomicInteger(0);
    private volatile boolean suspended = false;
    private volatile long suspendStartTime = 0;
    private volatile String lastError = "";

    // 监控计数器
    private final AtomicLong gradientRetryCount = new AtomicLong(0);
    private final AtomicLong gradientRetryRecoverCount = new AtomicLong(0);
    private final AtomicLong suspendCount = new AtomicLong(0);

    public GradientRetryManager() {
        this(DEFAULT_MAX_RETRY, DEFAULT_RETRY_DELAYS);
    }

    public GradientRetryManager(int maxRetry, int[] retryDelays) {
        if (maxRetry <= 0) {
            throw new IllegalArgumentException("maxRetry 必须 > 0，当前值: " + maxRetry);
        }
        if (retryDelays == null || retryDelays.length == 0) {
            throw new IllegalArgumentException("retryDelays 不能为空");
        }
        this.maxRetry = maxRetry;
        this.retryDelays = retryDelays;
    }

    /**
     * 带梯度重试的写入操作
     *
     * @param writeFn 实际写入函数，返回 true=成功，false=失败
     * @param errorContext 错误上下文描述
     * @return true=最终写入成功，false=全部重试耗尽（任务已暂停）
     */
    public boolean executeWithGradientRetry(Supplier<Boolean> writeFn, String errorContext) {
        // 首次尝试
        if (Boolean.TRUE.equals(writeFn.get())) {
            resetRetry();
            return true;
        }

        // 进入梯度重试
        for (int i = 0; i < maxRetry; i++) {
            int delaySeconds = i < retryDelays.length
                    ? retryDelays[i]
                    : retryDelays[retryDelays.length - 1];
            currentRetryLevel.set(i + 1);
            gradientRetryCount.incrementAndGet();

            log.warn("目标集群写入失败，第 {} 次梯度重试，等待 {}s 后重试。错误：{}",
                    i + 1, delaySeconds, errorContext);

            try {
                TimeUnit.SECONDS.sleep(delaySeconds);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            if (Boolean.TRUE.equals(writeFn.get())) {
                log.info("目标集群写入在第 {} 次重试后恢复成功", i + 1);
                gradientRetryRecoverCount.incrementAndGet();
                resetRetry();
                return true;
            }
        }

        // 全部重试耗尽 → 暂停任务
        suspend(errorContext);
        return false;
    }

    /**
     * 暂停同步任务
     */
    private void suspend(String errorMsg) {
        suspended = true;
        suspendStartTime = System.currentTimeMillis();
        lastError = errorMsg;
        suspendCount.incrementAndGet();

        log.error("目标集群写入持续失败，梯度重试 {} 次全部耗尽，同步任务已暂停。"
                + "最后错误：{}。请检查目标集群状态后通过 POST /resume 恢复",
                maxRetry, errorMsg);
    }

    /**
     * 手动恢复任务（由 POST /resume 接口调用）
     */
    public void resume() {
        suspended = false;
        suspendStartTime = 0;
        lastError = "";
        resetRetry();
        log.info("同步任务已手动恢复");
    }

    /**
     * 暂停状态周期性告警（建议每 30s 调用一次）
     */
    public void periodicSuspendAlert() {
        if (suspended) {
            long durationSec = (System.currentTimeMillis() - suspendStartTime) / 1000;
            log.error("同步任务已暂停：原因={}，暂停时长={}秒，请通过 POST /resume 恢复",
                    lastError, durationSec);
        }
    }

    private void resetRetry() {
        currentRetryLevel.set(0);
    }

    // ==================== Getters for metrics ====================

    public boolean isSuspended() {
        return suspended;
    }

    public long getSuspendCount() {
        return suspendCount.get();
    }

    public long getSuspendDurationSeconds() {
        return suspended ? (System.currentTimeMillis() - suspendStartTime) / 1000 : 0;
    }

    public long getGradientRetryCount() {
        return gradientRetryCount.get();
    }

    public long getGradientRetryRecoverCount() {
        return gradientRetryRecoverCount.get();
    }

    public int getLastRetryLevel() {
        return currentRetryLevel.get();
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public int[] getRetryDelays() {
        return retryDelays;
    }

    public String getLastError() {
        return lastError;
    }

    /**
     * 解析逗号分隔的延迟配置字符串
     *
     * @param delaysStr 逗号分隔的秒数，如 "1,3,10,30,60"
     * @return int 数组
     */
    public static int[] parseRetryDelays(String delaysStr) {
        if (delaysStr == null || delaysStr.trim().isEmpty()) {
            return DEFAULT_RETRY_DELAYS;
        }
        String[] parts = delaysStr.trim().split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i].trim());
            if (result[i] <= 0) {
                throw new IllegalArgumentException("重试延迟必须 > 0，位置 " + i + ": " + parts[i]);
            }
        }
        return result;
    }
}
