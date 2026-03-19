package org.apache.rocketmq.hasync.sink;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * GradientRetryManager 单元测试（需求 21 §21.3）
 */
public class GradientRetryManagerTest {

    private GradientRetryManager manager;

    @Before
    public void setUp() {
        // 使用较短的延迟时间便于测试（0 秒）
        manager = new GradientRetryManager(3, new int[]{0, 0, 0});
    }

    @Test
    public void testFirstAttemptSuccess() {
        boolean result = manager.executeWithGradientRetry(() -> true, "test");
        assertTrue("首次成功应返回 true", result);
        assertFalse("不应暂停", manager.isSuspended());
        assertEquals("重试计数应为 0", 0, manager.getGradientRetryCount());
        assertEquals("恢复计数应为 0", 0, manager.getGradientRetryRecoverCount());
    }

    @Test
    public void testSuccessOnSecondRetry() {
        AtomicInteger callCount = new AtomicInteger(0);
        boolean result = manager.executeWithGradientRetry(() -> {
            int count = callCount.incrementAndGet();
            return count >= 3; // 第 3 次调用成功（首次 + 第 2 次重试）
        }, "test");

        assertTrue("应最终成功", result);
        assertFalse("不应暂停", manager.isSuspended());
        assertEquals("重试触发 2 次", 2, manager.getGradientRetryCount());
        assertEquals("恢复 1 次", 1, manager.getGradientRetryRecoverCount());
    }

    @Test
    public void testAllRetriesExhausted_Suspended() {
        boolean result = manager.executeWithGradientRetry(() -> false, "写入超时");

        assertFalse("全部重试耗尽应返回 false", result);
        assertTrue("应暂停", manager.isSuspended());
        assertEquals("暂停次数 1", 1, manager.getSuspendCount());
        assertEquals("重试触发 3 次", 3, manager.getGradientRetryCount());
        assertEquals("最后重试级别 3", 3, manager.getLastRetryLevel());
        assertTrue("暂停持续时长应 >= 0", manager.getSuspendDurationSeconds() >= 0);
    }

    @Test
    public void testResume() {
        // 先触发暂停
        manager.executeWithGradientRetry(() -> false, "错误");
        assertTrue(manager.isSuspended());

        // 恢复
        manager.resume();
        assertFalse("恢复后不应暂停", manager.isSuspended());
        assertEquals("恢复后暂停时长为 0", 0, manager.getSuspendDurationSeconds());
        assertEquals("恢复后重试级别为 0", 0, manager.getLastRetryLevel());
    }

    @Test
    public void testMultipleSuspendAndResume() {
        // 第一次暂停
        manager.executeWithGradientRetry(() -> false, "错误1");
        assertEquals(1, manager.getSuspendCount());
        manager.resume();

        // 第二次暂停
        manager.executeWithGradientRetry(() -> false, "错误2");
        assertEquals(2, manager.getSuspendCount());
    }

    @Test
    public void testPeriodicSuspendAlert_WhenSuspended() {
        manager.executeWithGradientRetry(() -> false, "test");
        assertTrue(manager.isSuspended());
        // 不抛异常即可
        manager.periodicSuspendAlert();
    }

    @Test
    public void testPeriodicSuspendAlert_WhenNotSuspended() {
        assertFalse(manager.isSuspended());
        // 不抛异常即可
        manager.periodicSuspendAlert();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMaxRetry() {
        new GradientRetryManager(0, new int[]{1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRetryDelays() {
        new GradientRetryManager(3, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyRetryDelays() {
        new GradientRetryManager(3, new int[]{});
    }

    @Test
    public void testDefaultConstructor() {
        GradientRetryManager defaultManager = new GradientRetryManager();
        assertEquals(GradientRetryManager.DEFAULT_MAX_RETRY, defaultManager.getMaxRetry());
        assertArrayEquals(GradientRetryManager.DEFAULT_RETRY_DELAYS, defaultManager.getRetryDelays());
    }

    @Test
    public void testParseRetryDelays() {
        int[] delays = GradientRetryManager.parseRetryDelays("1,3,10,30,60");
        assertArrayEquals(new int[]{1, 3, 10, 30, 60}, delays);
    }

    @Test
    public void testParseRetryDelays_Null() {
        int[] delays = GradientRetryManager.parseRetryDelays(null);
        assertArrayEquals(GradientRetryManager.DEFAULT_RETRY_DELAYS, delays);
    }

    @Test
    public void testParseRetryDelays_Empty() {
        int[] delays = GradientRetryManager.parseRetryDelays("");
        assertArrayEquals(GradientRetryManager.DEFAULT_RETRY_DELAYS, delays);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseRetryDelays_Invalid() {
        GradientRetryManager.parseRetryDelays("1,0,3");
    }

    @Test
    public void testGetLastError() {
        manager.executeWithGradientRetry(() -> false, "网络超时");
        assertEquals("网络超时", manager.getLastError());

        manager.resume();
        assertEquals("", manager.getLastError());
    }

    @Test
    public void testRetryDelaysFallback() {
        // retryDelays 长度 < maxRetry 时，最后一个值作为 fallback
        GradientRetryManager m = new GradientRetryManager(5, new int[]{0, 0});
        boolean result = m.executeWithGradientRetry(() -> false, "test");
        assertFalse(result);
        assertTrue(m.isSuspended());
        assertEquals(5, m.getGradientRetryCount());
    }
}
