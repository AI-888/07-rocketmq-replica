package org.apache.rocketmq.hasync.checkpoint;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 启动一致性校验器 — Sink 启动时校验数据完整性
 * <p>
 * 对应需求 10 §6-11：
 * <ul>
 *   <li>从 confirmedOffset 后读取 X 条消息</li>
 *   <li>通过 ORIGIN_PHYSICAL_OFFSET 在目标集群查询匹配</li>
 *   <li>全部存在 → 跳过这 X 条，从第 X+1 条开始</li>
 *   <li>存在缺失 → 从 confirmedOffset 重新同步</li>
 * </ul>
 * <p>
 * 前置条件：
 * <ul>
 *   <li>startupCheckMsgCount > 0（否则跳过）</li>
 *   <li>confirmedOffset > 0（首次启动跳过）</li>
 * </ul>
 */
public class StartupConsistencyChecker {

    private static final Logger log = LoggerFactory.getLogger(StartupConsistencyChecker.class);

    private final int checkMsgCount;
    private MetricsCollector metricsCollector;

    /** 查询目标集群消息的回调 */
    private MessageExistenceChecker existenceChecker;

    /**
     * 查询消息是否已存在于目标集群
     */
    public interface MessageExistenceChecker {
        /**
         * 检查具有指定 physicOffset 的消息是否已写入目标集群
         *
         * @param topic         消息 Topic
         * @param physicOffset  源集群物理偏移量
         * @return true 表示已存在
         */
        boolean messageExists(String topic, long physicOffset);
    }

    /**
     * 校验结果
     */
    public static class CheckResult {
        private final String status;  // PASSED / FAILED / SKIPPED
        private final int msgFound;
        private final long resumeOffset;
        private final String message;

        public CheckResult(String status, int msgFound, long resumeOffset, String message) {
            this.status = status;
            this.msgFound = msgFound;
            this.resumeOffset = resumeOffset;
            this.message = message;
        }

        public String getStatus() { return status; }
        public int getMsgFound() { return msgFound; }
        public long getResumeOffset() { return resumeOffset; }
        public String getMessage() { return message; }

        @Override
        public String toString() {
            return "CheckResult{status='" + status + "', msgFound=" + msgFound
                    + ", resumeOffset=" + resumeOffset + ", message='" + message + "'}";
        }
    }

    public StartupConsistencyChecker(int checkMsgCount) {
        this.checkMsgCount = checkMsgCount;
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    public void setExistenceChecker(MessageExistenceChecker existenceChecker) {
        this.existenceChecker = existenceChecker;
    }

    /**
     * 执行启动一致性校验
     *
     * @param confirmedOffset   上次确认的位点
     * @param messagesAfterCheckpoint  confirmedOffset 之后的消息列表
     * @return 校验结果
     */
    public CheckResult check(long confirmedOffset, List<SyncRecord> messagesAfterCheckpoint) {
        // 前置条件检查
        if (checkMsgCount <= 0) {
            log.info("启动一致性校验已跳过（startupCheckMsgCount=0）");
            updateMetrics("SKIPPED", 0);
            return new CheckResult("SKIPPED", 0, confirmedOffset,
                    "startupCheckMsgCount=0，跳过校验");
        }

        if (confirmedOffset <= 0) {
            log.info("启动一致性校验已跳过（首次启动，confirmedOffset=0）");
            updateMetrics("SKIPPED", 0);
            return new CheckResult("SKIPPED", 0, 0,
                    "首次启动，无需校验");
        }

        if (messagesAfterCheckpoint == null || messagesAfterCheckpoint.isEmpty()) {
            log.info("启动一致性校验已跳过（无可校验消息）");
            updateMetrics("SKIPPED", 0);
            return new CheckResult("SKIPPED", 0, confirmedOffset,
                    "无可校验消息");
        }

        if (existenceChecker == null) {
            log.warn("启动一致性校验已跳过（existenceChecker 未设置）");
            updateMetrics("SKIPPED", 0);
            return new CheckResult("SKIPPED", 0, confirmedOffset,
                    "existenceChecker 未设置");
        }

        // 逐条检查消息是否已存在于目标集群
        int checkedCount = Math.min(checkMsgCount, messagesAfterCheckpoint.size());
        int foundCount = 0;

        for (int i = 0; i < checkedCount; i++) {
            SyncRecord record = messagesAfterCheckpoint.get(i);
            boolean exists = existenceChecker.messageExists(record.getTopic(), record.getPhysicOffset());

            if (exists) {
                foundCount++;
            } else {
                // 发现缺失 → 从 confirmedOffset 重新同步
                log.warn("启动一致性校验失败：第 {} 条消息缺失（topic={}, physicOffset={}），" +
                                "将从 confirmedOffset={} 重新同步",
                        i + 1, record.getTopic(), record.getPhysicOffset(), confirmedOffset);
                updateMetrics("FAILED", foundCount);
                return new CheckResult("FAILED", foundCount, confirmedOffset,
                        "第 " + (i + 1) + " 条消息缺失，从 confirmedOffset 重新同步");
            }
        }

        // 全部存在 → 跳过这些消息，从第 X+1 条开始
        SyncRecord lastChecked = messagesAfterCheckpoint.get(checkedCount - 1);
        long resumeOffset = lastChecked.getEndOffset();

        log.info("启动一致性校验通过：全部 {} 条消息已存在于目标集群，" +
                        "从 offset={} 开始续传",
                foundCount, resumeOffset);

        updateMetrics("PASSED", foundCount);
        return new CheckResult("PASSED", foundCount, resumeOffset,
                "全部 " + foundCount + " 条消息已存在，跳过重复消息");
    }

    private void updateMetrics(String status, int msgFound) {
        if (metricsCollector != null) {
            metricsCollector.setStartupCheckResult(status);
            metricsCollector.setStartupCheckMsgFound(msgFound);
        }
    }

    public int getCheckMsgCount() {
        return checkMsgCount;
    }
}
