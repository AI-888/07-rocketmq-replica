package org.apache.rocketmq.hasync.checkpoint;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * StartupConsistencyChecker 单元测试
 */
public class StartupConsistencyCheckerTest {

    private MetricsCollector metricsCollector;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
    }

    @Test
    public void testSkipWhenCheckMsgCountZero() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(0);
        checker.setMetricsCollector(metricsCollector);

        StartupConsistencyChecker.CheckResult result = checker.check(1000L, createRecords(5));
        assertEquals("SKIPPED", result.getStatus());
    }

    @Test
    public void testSkipWhenConfirmedOffsetZero() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(10);
        checker.setMetricsCollector(metricsCollector);

        StartupConsistencyChecker.CheckResult result = checker.check(0L, createRecords(5));
        assertEquals("SKIPPED", result.getStatus());
    }

    @Test
    public void testSkipWhenNoMessages() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(10);
        checker.setMetricsCollector(metricsCollector);

        StartupConsistencyChecker.CheckResult result = checker.check(1000L, Collections.emptyList());
        assertEquals("SKIPPED", result.getStatus());
    }

    @Test
    public void testSkipWhenNoExistenceChecker() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(10);
        checker.setMetricsCollector(metricsCollector);

        StartupConsistencyChecker.CheckResult result = checker.check(1000L, createRecords(5));
        assertEquals("SKIPPED", result.getStatus());
    }

    @Test
    public void testAllMessagesExist() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(3);
        checker.setMetricsCollector(metricsCollector);
        checker.setExistenceChecker((topic, offset) -> true); // 所有消息都存在

        List<SyncRecord> records = createRecords(5);
        StartupConsistencyChecker.CheckResult result = checker.check(1000L, records);

        assertEquals("PASSED", result.getStatus());
        assertEquals(3, result.getMsgFound());
        // resumeOffset 应该是第 3 条消息的 endOffset
        assertEquals(records.get(2).getEndOffset(), result.getResumeOffset());
    }

    @Test
    public void testMessageMissing() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(5);
        checker.setMetricsCollector(metricsCollector);
        // 第 3 条消息不存在
        checker.setExistenceChecker((topic, offset) -> offset != 1200L);

        List<SyncRecord> records = createRecords(5);
        StartupConsistencyChecker.CheckResult result = checker.check(1000L, records);

        assertEquals("FAILED", result.getStatus());
        assertEquals(2, result.getMsgFound()); // 前 2 条存在
        assertEquals(1000L, result.getResumeOffset()); // 从 confirmedOffset 重新同步
    }

    @Test
    public void testFirstMessageMissing() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(5);
        checker.setMetricsCollector(metricsCollector);
        checker.setExistenceChecker((topic, offset) -> false); // 所有消息都不存在

        StartupConsistencyChecker.CheckResult result = checker.check(1000L, createRecords(5));

        assertEquals("FAILED", result.getStatus());
        assertEquals(0, result.getMsgFound());
    }

    @Test
    public void testCheckCountLargerThanAvailable() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(100);
        checker.setMetricsCollector(metricsCollector);
        checker.setExistenceChecker((topic, offset) -> true);

        List<SyncRecord> records = createRecords(3); // 只有 3 条
        StartupConsistencyChecker.CheckResult result = checker.check(1000L, records);

        assertEquals("PASSED", result.getStatus());
        assertEquals(3, result.getMsgFound()); // 只检查了 3 条
    }

    @Test
    public void testMetricsUpdated() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(3);
        checker.setMetricsCollector(metricsCollector);
        checker.setExistenceChecker((topic, offset) -> true);

        checker.check(1000L, createRecords(5));
        assertEquals("PASSED", metricsCollector.getStartupCheckResult());
        assertEquals(3, metricsCollector.getStartupCheckMsgFound());
    }

    @Test
    public void testGetCheckMsgCount() {
        StartupConsistencyChecker checker = new StartupConsistencyChecker(42);
        assertEquals(42, checker.getCheckMsgCount());
    }

    @Test
    public void testCheckResultToString() {
        StartupConsistencyChecker.CheckResult result =
                new StartupConsistencyChecker.CheckResult("PASSED", 5, 2000L, "all good");
        String str = result.toString();
        assertTrue(str.contains("PASSED"));
        assertTrue(str.contains("2000"));
    }

    // ==================== 辅助方法 ====================

    private List<SyncRecord> createRecords(int count) {
        List<SyncRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SyncRecord record = new SyncRecord();
            record.setTopic("TestTopic");
            record.setPhysicOffset(1000L + i * 100);
            record.setEndOffset(1000L + (i + 1) * 100);
            record.setQueueId(0);
            record.setBody(new byte[64]);
            records.add(record);
        }
        return records;
    }
}
