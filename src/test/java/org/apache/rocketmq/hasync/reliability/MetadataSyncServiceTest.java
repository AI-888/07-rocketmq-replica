package org.apache.rocketmq.hasync.reliability;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * MetadataSyncService 单元测试
 */
public class MetadataSyncServiceTest {

    private MetadataSyncService service;
    private MetricsCollector metricsCollector;
    private int syncCallCount;
    private boolean syncShouldFail;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        service = new MetadataSyncService(60000);
        service.setMetricsCollector(metricsCollector);
        syncCallCount = 0;
        syncShouldFail = false;

        service.setSyncCallback(new MetadataSyncService.MetadataSyncCallback() {
            @Override
            public boolean syncMetadata(String metadataType) throws Exception {
                syncCallCount++;
                if (syncShouldFail) {
                    throw new RuntimeException("模拟同步失败");
                }
                return true;
            }

            @Override
            public long getDataVersion(String metadataType) {
                return 1L; // 固定版本号
            }
        });
    }

    @Test
    public void testSyncAllSuccess() {
        service.syncAll();
        assertEquals(6, syncCallCount); // 6 种元数据类型（含 TIMER_METRICS）
        assertTrue(service.getSyncSuccessCount() > 0);
        assertEquals(0, service.getSyncErrorCount());
    }

    @Test
    public void testSyncAllFailure() {
        syncShouldFail = true;
        service.syncAll();
        assertEquals(6, syncCallCount);
        assertEquals(0, service.getSyncSuccessCount());
        assertEquals(6, service.getSyncErrorCount());
    }

    @Test
    public void testSyncSkipsUnchangedVersion() {
        // 第一次同步
        service.syncAll();
        int firstCallCount = syncCallCount;

        // 第二次同步，DataVersion 未变化 → 跳过
        service.syncAll();
        assertEquals(firstCallCount, syncCallCount); // 调用次数没有增加
    }

    @Test
    public void testNoCallback() {
        MetadataSyncService noCallbackService = new MetadataSyncService(60000);
        noCallbackService.syncAll(); // 不应抛出异常
    }

    @Test
    public void testGetLastSyncTime() {
        service.syncAll();
        assertFalse(service.getLastSyncTime().isEmpty());
    }

    @Test
    public void testMetricsUpdated() {
        service.syncAll();
        assertTrue(metricsCollector.getMetaSyncSuccessCount() > 0);
        assertFalse(metricsCollector.getLastMetaSyncTime().isEmpty());
    }

    @Test
    public void testStartAndStop() throws Exception {
        service.start();
        Thread.sleep(100);
        service.stop();
        // 不应抛出异常
    }

    @Test
    public void testStopWithoutStart() {
        service.stop(); // 不应抛出异常
    }

    @Test
    public void testDataVersions() {
        service.syncAll();
        assertEquals(6, service.getDataVersions().size());
    }

    @Test
    public void testMetadataTypes() {
        assertEquals(6, MetadataSyncService.METADATA_TYPES.length);
    }
}
