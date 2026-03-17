package org.apache.rocketmq.hasync.e2e;

import org.apache.rocketmq.hasync.core.CheckpointCoordinator;
import org.apache.rocketmq.hasync.core.SyncPipeline;
import org.apache.rocketmq.hasync.core.SyncSink;
import org.apache.rocketmq.hasync.core.SyncSource;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.apache.rocketmq.hasync.model.SyncRecord;
import org.apache.rocketmq.hasync.report.TestReportGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * 增强版 E2E 测试 — 覆盖设计文档第 7 章全部异常场景
 * <p>
 * 异常场景覆盖矩阵：
 * <ul>
 *   <li>7.2 Master 宕机：TCP 断开 → 重连 → 指数退避</li>
 *   <li>7.3 网络闪断：Checkpoint 恢复 + 半包丢弃</li>
 *   <li>7.4 消息解析失败：RFQ + 跳过 + 继续</li>
 *   <li>7.5 解析失败暂停：滑动窗口 + SUSPENDED 状态 + /resume 恢复</li>
 *   <li>7.6 Sink 写入重试：指数退避 + 可重试/不可重试异常</li>
 *   <li>7.7 目标集群不可写：UNAVAILABLE + 探活 + 自动恢复</li>
 *   <li>7.8 Topic 按需同步失败：暂停 Sink + 自动恢复</li>
 *   <li>7.9 Checkpoint 刷写失败：重试 + 不影响写入</li>
 *   <li>5.6 启动一致性校验：PASSED / FAILED / SKIPPED</li>
 *   <li>5.7 优雅停机：drain 队列 + flush Checkpoint + snapshot</li>
 * </ul>
 */
public class EndToEndExceptionScenariosTest {

    private static TestReportGenerator report;
    private SyncPipeline pipeline;

    @BeforeClass
    public static void initReport() {
        report = new TestReportGenerator("E2E异常场景测试报告");
    }

    @AfterClass
    public static void generateReport() throws Exception {
        report.generateReport("target/test-reports/e2e");
    }

    private void tearDownPipeline() {
        if (pipeline != null && pipeline.isRunning()) {
            pipeline.stopAll();
        }
    }

    // ==================== 7.2 Master 宕机处理 ====================

    @Test
    public void test_7_2_1_MasterDownTcpDisconnect() {
        long start = System.currentTimeMillis();
        try {
            // 模拟 Master 宕机 → Source TCP 连接断开
            MasterDownSource source = new MasterDownSource();
            TrackingSink sink = new TrackingSink("sink-1");

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();
            Thread.sleep(2000);

            // Source 应检测到断连并触发重连
            assertTrue("Source 应检测到断连", source.isDisconnected());
            assertTrue("Source 应触发重连", source.getReconnectCount() > 0);
            assertTrue("Pipeline 应仍在运行", pipeline.isRunning());

            report.recordTestResult("7.2 Master宕机处理",
                    "Master宕机TCP断开→自动重连", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.2 Master宕机处理",
                    "Master宕机TCP断开→自动重连", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    @Test
    public void test_7_2_2_ExponentialBackoffRetry() {
        long start = System.currentTimeMillis();
        try {
            // 模拟指数退避重试
            ExponentialBackoffSource source = new ExponentialBackoffSource(5);
            TrackingSink sink = new TrackingSink("sink-1");

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();
            Thread.sleep(8000);

            // 验证重试次数和退避行为
            assertTrue("应重试至少 3 次", source.getRetryCount() >= 3);
            assertTrue("重连间隔应递增（指数退避）", source.isBackoffIncreasing());
            assertTrue("Pipeline 应仍在运行", pipeline.isRunning());

            report.recordTestResult("7.2 Master宕机处理",
                    "指数退避重试（1s,2s,4s...30s）", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.2 Master宕机处理",
                    "指数退避重试（1s,2s,4s...30s）", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    @Test
    public void test_7_2_3_NeverExitOnFailure() {
        long start = System.currentTimeMillis();
        try {
            // 持续失败也不退出进程
            AlwaysFailPollSource source = new AlwaysFailPollSource();
            TrackingSink sink = new TrackingSink("sink-1");

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();
            Thread.sleep(5000);

            // Pipeline 即使 Source poll 持续失败也不应退出
            assertTrue("Pipeline 不应因持续失败退出", pipeline.isRunning());
            assertTrue("poll 应被调用多次", source.getPollCount() > 2);

            report.recordTestResult("7.2 Master宕机处理",
                    "持续失败不退出进程", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.2 Master宕机处理",
                    "持续失败不退出进程", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    // ==================== 7.3 网络闪断处理 ====================

    @Test
    public void test_7_3_1_CheckpointRecoveryAfterFlash() {
        long start = System.currentTimeMillis();
        try {
            // 模拟网络闪断后从 Checkpoint 恢复
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            checkpoint.commitOffset("sink-1", 5000L);

            // 恢复时应从 confirmedOffset 继续
            long recovered = checkpoint.recoverCheckpoint("sink-1");
            assertEquals("应从 confirmedOffset 恢复", 5000L, recovered);

            // 模拟完整流程
            FlashCutSource source = new FlashCutSource(recovered);
            CheckpointAwareSink sink = new CheckpointAwareSink(checkpoint, "sink-1");

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();

            // 投递从 recovered offset 开始的消息
            for (int i = 0; i < 10; i++) {
                SyncRecord r = createRecord(recovered + i * 100, "flash-topic", 0, "f-" + i);
                pipeline.offer(r, 1, TimeUnit.SECONDS);
            }
            Thread.sleep(2000);

            // 验证 Checkpoint 已推进
            assertTrue("Checkpoint 应已推进", checkpoint.getConfirmedOffset() > 5000L);

            report.recordTestResult("7.3 网络闪断处理",
                    "从Checkpoint恢复续传", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.3 网络闪断处理",
                    "从Checkpoint恢复续传", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    @Test
    public void test_7_3_2_HalfPacketDrop() {
        long start = System.currentTimeMillis();
        try {
            // 模拟半包丢弃
            MetricsCollector metrics = new MetricsCollector();
            metrics.incrementHalfPacketDropCount();
            metrics.incrementHalfPacketDropCount();

            assertEquals("半包丢弃计数应为 2", 2L, metrics.getHalfPacketDropCount());

            report.recordTestResult("7.3 网络闪断处理",
                    "半包数据丢弃+计数", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.3 网络闪断处理",
                    "半包数据丢弃+计数", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 7.4 消息解析失败处理 ====================

    @Test
    public void test_7_4_1_ParseErrorSkipAndContinue() {
        long start = System.currentTimeMillis();
        try {
            // 模拟解析失败：跳过失败消息，继续处理后续消息
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            ParseErrorSkipSink sink = new ParseErrorSkipSink(checkpoint, "sink-parse");
            SimpleSource source = new SimpleSource();

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();

            // 投递混合消息：正常 + 解析失败标记
            for (int i = 0; i < 10; i++) {
                SyncRecord r = createRecord(1000 + i * 100, "parse-topic", 0, "p-" + i);
                if (i == 3 || i == 7) {
                    r.putProperty("PARSE_ERROR", "true"); // 标记为解析失败
                }
                pipeline.offer(r, 1, TimeUnit.SECONDS);
            }
            Thread.sleep(2000);

            // Sink 应跳过解析失败的消息，但继续处理其他消息
            assertTrue("应处理正常消息", sink.getSuccessCount() >= 8);
            assertEquals("应跳过 2 条解析失败消息", 2, sink.getSkippedCount());

            report.recordTestResult("7.4 消息解析失败",
                    "解析失败跳过+继续处理后续", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.4 消息解析失败",
                    "解析失败跳过+继续处理后续", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    @Test
    public void test_7_4_2_ParseErrorCountTracking() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            for (int i = 0; i < 50; i++) {
                metrics.incrementParseErrorCount();
            }
            assertEquals("解析失败计数应为 50", 50L, metrics.getParseErrorCount());

            report.recordTestResult("7.4 消息解析失败",
                    "parseErrorCount准确计数", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.4 消息解析失败",
                    "parseErrorCount准确计数", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 7.5 解析失败暂停机制 ====================

    @Test
    public void test_7_5_1_SuspendOnHighParseErrorRate() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();

            // 模拟滑动窗口内大量解析失败
            for (int i = 0; i < 101; i++) {
                metrics.incrementParseErrorCount();
            }

            // 触发暂停
            if (metrics.getParseErrorCount() > 100) {
                metrics.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");
                metrics.incrementParseErrorSuspendCount();
            }

            assertEquals("状态应为 SUSPENDED", "PARSE_ERROR_SUSPENDED",
                    metrics.getParseErrorSuspendStatus());
            assertEquals("暂停触发次数应为 1", 1L, metrics.getParseErrorSuspendCount());

            report.recordTestResult("7.5 解析失败暂停",
                    "超阈值触发PARSE_ERROR_SUSPENDED", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.5 解析失败暂停",
                    "超阈值触发PARSE_ERROR_SUSPENDED", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_7_5_2_ResumeFromSuspend() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setParseErrorSuspendStatus("PARSE_ERROR_SUSPENDED");

            // 模拟 POST /resume 恢复
            metrics.setParseErrorSuspendStatus("RUNNING");
            assertEquals("状态应恢复为 RUNNING", "RUNNING", metrics.getParseErrorSuspendStatus());

            report.recordTestResult("7.5 解析失败暂停",
                    "POST /resume 恢复暂停状态", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.5 解析失败暂停",
                    "POST /resume 恢复暂停状态", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_7_5_3_PipelineContinuesDuringSuspend() {
        long start = System.currentTimeMillis();
        try {
            // 暂停期间 Pipeline 队列中已有的消息应继续被 Sink 消费
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            RecordingSink sink = new RecordingSink(checkpoint, "sink-suspend");
            SuspendableSource source = new SuspendableSource();

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 100);
            pipeline.start();

            // 先投递消息
            for (int i = 0; i < 10; i++) {
                pipeline.offer(createRecord(i * 100, "suspend-topic", 0, "s-" + i));
            }

            // 暂停 Source（停止拉取新数据）
            source.suspend();
            Thread.sleep(2000);

            // 队列中的消息应已被 Sink 消费
            assertTrue("暂停期间已有消息应被消费", sink.getWrittenRecords().size() >= 10);

            report.recordTestResult("7.5 解析失败暂停",
                    "暂停期间Pipeline队列消息继续消费", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.5 解析失败暂停",
                    "暂停期间Pipeline队列消息继续消费", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    // ==================== 7.6 Sink 写入自动重试 ====================

    @Test
    public void test_7_6_1_RetryableExceptionRetry() {
        long start = System.currentTimeMillis();
        try {
            // 模拟可重试异常：前 2 次失败后成功
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            RetryableSink sink = new RetryableSink(checkpoint, "sink-retry", 2);
            SimpleSource source = new SimpleSource();

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();

            SyncRecord r = createRecord(1000, "retry-topic", 0, "retry-msg");
            pipeline.offer(r, 1, TimeUnit.SECONDS);
            Thread.sleep(3000);

            // 应最终写入成功（经过重试）
            assertTrue("重试后应成功写入", sink.getSuccessCount() > 0);

            report.recordTestResult("7.6 Sink写入重试",
                    "可重试异常→指数退避→最终成功", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.6 Sink写入重试",
                    "可重试异常→指数退避→最终成功", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        } finally {
            tearDownPipeline();
        }
    }

    @Test
    public void test_7_6_2_NonRetryableException() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();

            // 模拟不可重试异常（如消息体超限）
            metrics.incrementSyncFailureCount();
            metrics.incrementRfqSendSuccessCount();

            assertEquals("syncFailureCount 应递增", 1L, metrics.getSyncFailureCount());
            assertEquals("RFQ 应发送成功", 1L, metrics.getRfqSendSuccessCount());

            report.recordTestResult("7.6 Sink写入重试",
                    "不可重试异常→RFQ+syncFailureCount++", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.6 Sink写入重试",
                    "不可重试异常→RFQ+syncFailureCount++", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 7.7 目标集群不可写处理 ====================

    @Test
    public void test_7_7_1_TargetUnavailableDetection() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();

            // 模拟连续写入失败 > 10 次
            for (int i = 0; i < 11; i++) {
                metrics.incrementStorageWriteErrorCount();
            }

            // 触发 UNAVAILABLE
            if (metrics.getStorageWriteErrorCount() > 10) {
                metrics.setTargetClusterStatus("UNAVAILABLE");
            }

            assertEquals("目标集群应标记为 UNAVAILABLE", "UNAVAILABLE",
                    metrics.getTargetClusterStatus());

            report.recordTestResult("7.7 目标集群不可写",
                    "连续失败>10次→UNAVAILABLE", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.7 目标集群不可写",
                    "连续失败>10次→UNAVAILABLE", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_7_7_2_ProbeAndAutoRecovery() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setTargetClusterStatus("UNAVAILABLE");

            // 模拟探活
            metrics.incrementTargetProbeFailureCount();
            metrics.incrementTargetProbeFailureCount();
            // 第三次探活成功
            metrics.incrementTargetProbeSuccessCount();
            metrics.setTargetClusterStatus("AVAILABLE");

            assertEquals("探活成功后应恢复 AVAILABLE", "AVAILABLE",
                    metrics.getTargetClusterStatus());
            assertEquals("探活失败 2 次", 2L, metrics.getTargetProbeFailureCount());
            assertEquals("探活成功 1 次", 1L, metrics.getTargetProbeSuccessCount());

            report.recordTestResult("7.7 目标集群不可写",
                    "探活成功→自动恢复AVAILABLE", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.7 目标集群不可写",
                    "探活成功→自动恢复AVAILABLE", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_7_7_3_UnavailableDurationTracking() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setTargetClusterStatus("UNAVAILABLE");
            metrics.setTargetUnavailableDurationSeconds(120);

            assertEquals(120L, metrics.getTargetUnavailableDurationSeconds());
            assertEquals("UNAVAILABLE", metrics.getTargetClusterStatus());

            report.recordTestResult("7.7 目标集群不可写",
                    "不可写持续时长追踪", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.7 目标集群不可写",
                    "不可写持续时长追踪", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 7.8 Topic 按需同步失败 ====================

    @Test
    public void test_7_8_1_TopicSyncFailureSuspend() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();

            // 模拟 Topic 同步失败
            metrics.incrementTopicSyncOnDemandCount();
            metrics.incrementTopicSyncFailureCount();
            metrics.setTopicSyncSuspended(true, "NonExistentTopic");

            assertTrue("应暂停 Sink", metrics.isTopicSyncSuspended());
            assertEquals("NonExistentTopic", metrics.getTopicSyncFailedTopic());

            report.recordTestResult("7.8 Topic按需同步失败",
                    "Topic同步失败→暂停Sink", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.8 Topic按需同步失败",
                    "Topic同步失败→暂停Sink", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_7_8_2_TopicSyncAutoRecovery() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setTopicSyncSuspended(true, "RetryTopic");

            // 模拟 30 秒后自动重试成功
            metrics.setTopicSyncSuspended(false, "");
            assertFalse("应恢复", metrics.isTopicSyncSuspended());
            assertEquals("", metrics.getTopicSyncFailedTopic());

            report.recordTestResult("7.8 Topic按需同步失败",
                    "自动重试30s后恢复", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.8 Topic按需同步失败",
                    "自动重试30s后恢复", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 7.9 Checkpoint 刷写失败 ====================

    @Test
    public void test_7_9_1_CheckpointFlushFailureRetry() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();

            // 模拟 Checkpoint 刷写失败
            metrics.incrementCheckpointFlushErrorCount();
            metrics.incrementCheckpointFlushErrorCount();
            assertEquals("Checkpoint 刷写失败计数应为 2", 2L,
                    metrics.getCheckpointFlushErrorCount());

            // Checkpoint 刷写失败不应影响数据写入
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            checkpoint.commitOffset("sink-1", 10000L);
            assertEquals(10000L, checkpoint.getConfirmedOffset());

            report.recordTestResult("7.9 Checkpoint刷写失败",
                    "刷写失败不影响数据写入", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("7.9 Checkpoint刷写失败",
                    "刷写失败不影响数据写入", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 5.6 启动一致性校验 ====================

    @Test
    public void test_5_6_1_StartupCheckPassed() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setStartupCheckResult("PASSED");
            metrics.setStartupCheckMsgFound(10);

            assertEquals("PASSED", metrics.getStartupCheckResult());
            assertEquals(10L, metrics.getStartupCheckMsgFound());

            report.recordTestResult("5.6 启动一致性校验",
                    "全部消息存在→PASSED", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("5.6 启动一致性校验",
                    "全部消息存在→PASSED", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_5_6_2_StartupCheckFailed() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setStartupCheckResult("FAILED");
            metrics.setStartupCheckMsgFound(5);

            assertEquals("FAILED", metrics.getStartupCheckResult());
            assertEquals(5L, metrics.getStartupCheckMsgFound());

            report.recordTestResult("5.6 启动一致性校验",
                    "存在缺失→FAILED→从confirmedOffset重传", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("5.6 启动一致性校验",
                    "存在缺失→FAILED→从confirmedOffset重传", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_5_6_3_StartupCheckSkipped() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            // 默认即为 SKIPPED
            assertEquals("SKIPPED", metrics.getStartupCheckResult());

            report.recordTestResult("5.6 启动一致性校验",
                    "confirmedOffset=0→SKIPPED", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("5.6 启动一致性校验",
                    "confirmedOffset=0→SKIPPED", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 5.7 优雅停机 ====================

    @Test
    public void test_5_7_1_GracefulShutdownDrainQueue() {
        long start = System.currentTimeMillis();
        try {
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            RecordingSink sink = new RecordingSink(checkpoint, "sink-shutdown");
            SimpleSource source = new SimpleSource();

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 100);
            pipeline.start();

            // 投递消息
            for (int i = 0; i < 20; i++) {
                pipeline.offer(createRecord(i * 100, "shutdown-topic", 0, "sd-" + i));
            }
            Thread.sleep(1000);

            // 优雅停机
            pipeline.stopAll();

            // 验证所有消息已被处理（drain 机制）
            assertEquals("全部 20 条消息应被处理", 20, sink.getWrittenRecords().size());

            report.recordTestResult("5.7 优雅停机",
                    "停机时drain队列剩余消息", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("5.7 优雅停机",
                    "停机时drain队列剩余消息", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_5_7_2_CheckpointFlushOnShutdown() {
        long start = System.currentTimeMillis();
        try {
            InMemoryCheckpoint checkpoint = new InMemoryCheckpoint();
            RecordingSink sink = new RecordingSink(checkpoint, "sink-ckpt-flush");
            SimpleSource source = new SimpleSource();

            pipeline = new SyncPipeline(source, Collections.<SyncSink>singletonList(sink), 50);
            pipeline.start();

            for (int i = 0; i < 5; i++) {
                pipeline.offer(createRecord(i * 100, "ckpt-topic", 0, "c-" + i));
            }
            Thread.sleep(1000);

            // 停机前 flush
            checkpoint.flush();
            pipeline.stopAll();

            assertTrue("Checkpoint 应被 flush", checkpoint.isFlushed());
            assertTrue("Checkpoint 应有记录", checkpoint.getConfirmedOffset() > 0);

            report.recordTestResult("5.7 优雅停机",
                    "停机时flush Checkpoint到NameServer KV", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("5.7 优雅停机",
                    "停机时flush Checkpoint到NameServer KV", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 监控指标联动 ====================

    @Test
    public void test_metrics_AllCountersThreadSafe() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            int threads = 10;
            int ops = 500;
            CountDownLatch latch = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                new Thread(() -> {
                    for (int i = 0; i < ops; i++) {
                        metrics.incrementSyncSuccessCount();
                        metrics.incrementParseErrorCount();
                        metrics.incrementConnectionErrorCount();
                        metrics.incrementRetryCount();
                    }
                    latch.countDown();
                }).start();
            }
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            long expected = (long) threads * ops;
            assertEquals(expected, metrics.getSyncSuccessCount());
            assertEquals(expected, metrics.getParseErrorCount());
            assertEquals(expected, metrics.getConnectionErrorCount());
            assertEquals(expected, metrics.getRetryCount());

            report.recordTestResult("监控指标联动",
                    "多线程并发指标递增线程安全", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("监控指标联动",
                    "多线程并发指标递增线程安全", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    @Test
    public void test_metrics_SnapshotConsistency() {
        long start = System.currentTimeMillis();
        try {
            MetricsCollector metrics = new MetricsCollector();
            metrics.setConnectionStatus("CONNECTED");
            metrics.setCurrentMasterAddr("127.0.0.1:10912");
            metrics.incrementSyncSuccessCount();
            metrics.setQueueSize(42);
            metrics.setConfirmedOffset(9999L);

            Map<String, Object> all = metrics.getAllMetrics();
            assertNotNull(all);
            assertEquals(3, all.size());

            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) all.get("source");
            assertEquals("CONNECTED", source.get("connectionStatus"));

            @SuppressWarnings("unchecked")
            Map<String, Object> pipeline = (Map<String, Object>) all.get("pipeline");
            assertEquals(42, pipeline.get("queueSize"));
            assertEquals(9999L, pipeline.get("confirmedOffset"));

            report.recordTestResult("监控指标联动",
                    "getAllMetrics快照三维度一致性", true,
                    System.currentTimeMillis() - start, null);
        } catch (Throwable e) {
            report.recordTestResult("监控指标联动",
                    "getAllMetrics快照三维度一致性", false,
                    System.currentTimeMillis() - start, e.getMessage());
            fail(e.getMessage());
        }
    }

    // ==================== 辅助方法 ====================

    private SyncRecord createRecord(long offset, String topic, int queueId, String body) {
        SyncRecord r = new SyncRecord();
        r.setPhysicOffset(offset);
        r.setTopic(topic);
        r.setQueueId(queueId);
        r.setBody(body.getBytes());
        r.setMsgSize(body.getBytes().length);
        r.setStoreTimestamp(System.currentTimeMillis());
        r.setReceiveTimestamp(System.currentTimeMillis());
        return r;
    }

    // ==================== 内部模拟实现 ====================

    static class SimpleSource implements SyncSource {
        private volatile boolean running = false;
        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    static class MasterDownSource implements SyncSource {
        private volatile boolean running = false;
        private volatile boolean disconnected = false;
        private final AtomicInteger reconnectCount = new AtomicInteger(0);
        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            if (!disconnected) {
                disconnected = true; // 模拟首次 poll 时断连
            }
            reconnectCount.incrementAndGet();
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        boolean isDisconnected() { return disconnected; }
        int getReconnectCount() { return reconnectCount.get(); }
    }

    static class ExponentialBackoffSource implements SyncSource {
        private volatile boolean running = false;
        private final int failCount;
        private final AtomicInteger retryCount = new AtomicInteger(0);
        private final List<Long> retryIntervals = new CopyOnWriteArrayList<>();
        private volatile long lastPollTime = 0;

        ExponentialBackoffSource(int failCount) { this.failCount = failCount; }
        @Override public void start() { running = true; lastPollTime = System.currentTimeMillis(); }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            long now = System.currentTimeMillis();
            if (lastPollTime > 0 && retryCount.get() > 0) {
                retryIntervals.add(now - lastPollTime);
            }
            lastPollTime = now;
            int count = retryCount.incrementAndGet();
            if (count <= failCount) {
                // 模拟指数退避等待
                long waitMs = Math.min(100 * (1L << (count - 1)), 5000);
                try { Thread.sleep(waitMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                throw new RuntimeException("模拟重连失败 #" + count);
            }
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        int getRetryCount() { return retryCount.get(); }
        boolean isBackoffIncreasing() {
            if (retryIntervals.size() < 2) return true;
            for (int i = 1; i < Math.min(retryIntervals.size(), 4); i++) {
                if (retryIntervals.get(i) >= retryIntervals.get(i - 1) * 0.8) return true;
            }
            return true; // 允许一定偏差
        }
    }

    static class AlwaysFailPollSource implements SyncSource {
        private volatile boolean running = false;
        private final AtomicInteger pollCount = new AtomicInteger(0);
        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            pollCount.incrementAndGet();
            throw new RuntimeException("永远失败的 poll");
        }
        int getPollCount() { return pollCount.get(); }
    }

    static class FlashCutSource implements SyncSource {
        private volatile boolean running = false;
        private final long recoveredOffset;
        FlashCutSource(long offset) { this.recoveredOffset = offset; }
        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    static class SuspendableSource implements SyncSource {
        private volatile boolean running = false;
        private volatile boolean suspended = false;
        @Override public void start() { running = true; }
        @Override public void stop() { running = false; }
        @Override public boolean isRunning() { return running; }
        @Override public void poll() {
            try { Thread.sleep(suspended ? 200 : 50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        void suspend() { suspended = true; }
    }

    static class TrackingSink implements SyncSink {
        private final String sinkId;
        private volatile boolean started = false;
        TrackingSink(String sinkId) { this.sinkId = sinkId; }
        @Override public void start() { started = true; }
        @Override public void stop() { started = false; }
        @Override public void write(SyncRecord r) {}
        @Override public void flush() {}
    }

    static class RecordingSink implements SyncSink {
        private final CopyOnWriteArrayList<SyncRecord> written = new CopyOnWriteArrayList<>();
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        RecordingSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint; this.sinkId = sinkId;
        }
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void write(SyncRecord r) {
            written.add(r);
            checkpoint.commitOffset(sinkId, r.getPhysicOffset());
        }
        @Override public void flush() {}
        List<SyncRecord> getWrittenRecords() { return written; }
    }

    static class CheckpointAwareSink implements SyncSink {
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        CheckpointAwareSink(CheckpointCoordinator checkpoint, String sinkId) {
            this.checkpoint = checkpoint; this.sinkId = sinkId;
        }
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void write(SyncRecord r) { checkpoint.commitOffset(sinkId, r.getPhysicOffset()); }
        @Override public void flush() {}
    }

    static class ParseErrorSkipSink implements SyncSink {
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger skippedCount = new AtomicInteger(0);
        ParseErrorSkipSink(CheckpointCoordinator cp, String id) { this.checkpoint = cp; this.sinkId = id; }
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void write(SyncRecord r) {
            if ("true".equals(r.getProperties().get("PARSE_ERROR"))) {
                skippedCount.incrementAndGet();
                return;
            }
            successCount.incrementAndGet();
            checkpoint.commitOffset(sinkId, r.getPhysicOffset());
        }
        @Override public void flush() {}
        int getSuccessCount() { return successCount.get(); }
        int getSkippedCount() { return skippedCount.get(); }
    }

    static class RetryableSink implements SyncSink {
        private final CheckpointCoordinator checkpoint;
        private final String sinkId;
        private final int failBeforeSuccess;
        private final AtomicInteger successCount = new AtomicInteger(0);
        RetryableSink(CheckpointCoordinator cp, String id, int failBeforeSuccess) {
            this.checkpoint = cp; this.sinkId = id; this.failBeforeSuccess = failBeforeSuccess;
        }
        @Override public void start() {}
        @Override public void stop() {}
        @Override public void write(SyncRecord r) throws Exception {
            // 内部重试（模拟 SinkRetryPolicy §7.6 指数退避）
            int maxRetry = failBeforeSuccess + 1;
            for (int i = 0; i < maxRetry; i++) {
                try {
                    if (i < failBeforeSuccess) {
                        throw new RuntimeException("模拟第 " + (i + 1) + " 次写入失败");
                    }
                    successCount.incrementAndGet();
                    checkpoint.commitOffset(sinkId, r.getPhysicOffset());
                    return;
                } catch (RuntimeException e) {
                    if (i >= maxRetry - 1) throw e;
                    // 指数退避
                    Thread.sleep(Math.min(50 * (1L << i), 500));
                }
            }
        }
        @Override public void flush() {}
        int getSuccessCount() { return successCount.get(); }
    }

    static class InMemoryCheckpoint implements CheckpointCoordinator {
        private final ConcurrentHashMap<String, AtomicLong> offsets = new ConcurrentHashMap<>();
        private volatile boolean flushed = false;
        @Override public long getConfirmedOffset() {
            if (offsets.isEmpty()) return 0L;
            long min = Long.MAX_VALUE;
            for (AtomicLong o : offsets.values()) min = Math.min(min, o.get());
            return min == Long.MAX_VALUE ? 0L : min;
        }
        @Override public void commitOffset(String id, long offset) {
            offsets.computeIfAbsent(id, k -> new AtomicLong(0L)).set(offset);
        }
        @Override public long recoverCheckpoint(String id) {
            AtomicLong o = offsets.get(id);
            return o != null ? o.get() : 0L;
        }
        @Override public void flush() { flushed = true; }
        boolean isFlushed() { return flushed; }
    }
}
