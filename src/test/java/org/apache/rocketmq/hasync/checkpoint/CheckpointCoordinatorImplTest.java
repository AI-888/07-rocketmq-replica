package org.apache.rocketmq.hasync.checkpoint;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

/**
 * CheckpointCoordinatorImpl 单元测试
 */
public class CheckpointCoordinatorImplTest {

    private CheckpointCoordinatorImpl coordinator;
    private MetricsCollector metricsCollector;
    private Map<String, String> kvStore;

    @Before
    public void setUp() {
        kvStore = new ConcurrentHashMap<>();
        metricsCollector = new MetricsCollector();
        coordinator = new CheckpointCoordinatorImpl("broker-a", 1000, 100);
        coordinator.setMetricsCollector(metricsCollector);
        coordinator.setPersistCallback(new CheckpointCoordinatorImpl.CheckpointPersistCallback() {
            @Override
            public void putKVConfig(String namespace, String key, String value) {
                kvStore.put(namespace + ":" + key, value);
            }

            @Override
            public String getKVConfig(String namespace, String key) {
                return kvStore.get(namespace + ":" + key);
            }
        });
    }

    @Test
    public void testCommitOffset() {
        coordinator.commitOffset("sink-1", 1000L);
        assertEquals(1000L, coordinator.getConfirmedOffset());
    }

    @Test
    public void testMultipleSinkGlobalCheckpoint() {
        coordinator.commitOffset("sink-1", 1000L);
        coordinator.commitOffset("sink-2", 500L);
        coordinator.commitOffset("sink-3", 1500L);
        // globalCheckpoint = min(1000, 500, 1500) = 500
        assertEquals(500L, coordinator.getGlobalCheckpoint());
    }

    @Test
    public void testFlush() {
        coordinator.commitOffset("sink-1", 1000L);
        coordinator.commitOffset("sink-2", 2000L);
        coordinator.flush();

        // 验证 KV 中写入了正确的值
        assertEquals("1000", kvStore.get("SYNC_CHECKPOINT:broker-a:sink:sink-1:commitOffset"));
        assertEquals("2000", kvStore.get("SYNC_CHECKPOINT:broker-a:sink:sink-2:commitOffset"));
        assertEquals("1000", kvStore.get("SYNC_CHECKPOINT:broker-a:globalCheckpoint"));
    }

    @Test
    public void testRecoverCheckpoint() {
        // 预置 KV 数据
        kvStore.put("SYNC_CHECKPOINT:broker-a:sink:sink-1:commitOffset", "5000");
        long recovered = coordinator.recoverCheckpoint("sink-1");
        assertEquals(5000L, recovered);
    }

    @Test
    public void testRecoverCheckpointNotFound() {
        long recovered = coordinator.recoverCheckpoint("sink-nonexist");
        assertEquals(0L, recovered);
    }

    @Test
    public void testRecoverCheckpointNoPersistCallback() {
        CheckpointCoordinatorImpl noPersist = new CheckpointCoordinatorImpl("broker-b", 1000, 100);
        long recovered = noPersist.recoverCheckpoint("sink-1");
        assertEquals(0L, recovered);
    }

    @Test
    public void testFlushWithNoPersistCallback() {
        CheckpointCoordinatorImpl noPersist = new CheckpointCoordinatorImpl("broker-b", 1000, 100);
        noPersist.commitOffset("sink-1", 100L);
        // 不应抛出异常
        noPersist.flush();
    }

    @Test
    public void testFlushErrorCounting() {
        coordinator.setPersistCallback(new CheckpointCoordinatorImpl.CheckpointPersistCallback() {
            @Override
            public void putKVConfig(String namespace, String key, String value) throws Exception {
                throw new RuntimeException("模拟刷写失败");
            }

            @Override
            public String getKVConfig(String namespace, String key) {
                return null;
            }
        });

        coordinator.commitOffset("sink-1", 100L);
        coordinator.flush();

        assertEquals(1L, coordinator.getFlushErrorCount());
    }

    @Test
    public void testProgressiveCommit() {
        // 模拟 Sink 不断推进 offset
        for (long i = 100; i <= 1000; i += 100) {
            coordinator.commitOffset("sink-1", i);
        }
        assertEquals(1000L, coordinator.getConfirmedOffset());
    }

    @Test
    public void testSinkCommitOffsetsMap() {
        coordinator.commitOffset("sink-1", 100L);
        coordinator.commitOffset("sink-2", 200L);
        ConcurrentHashMap<String, Long> offsets = coordinator.getSinkCommitOffsets();
        assertEquals(2, offsets.size());
        assertEquals(Long.valueOf(100L), offsets.get("sink-1"));
        assertEquals(Long.valueOf(200L), offsets.get("sink-2"));
    }

    @Test
    public void testBrokerName() {
        assertEquals("broker-a", coordinator.getBrokerName());
    }

    @Test
    public void testStartAndStop() throws Exception {
        coordinator.start();
        Thread.sleep(100); // 给调度器一点时间
        coordinator.stop();
        // 不应抛出异常
    }

    @Test
    public void testPendingPacketCount() {
        assertEquals(0, coordinator.getPendingPacketCount());
        coordinator.commitOffset("sink-1", 100L);
        // pending count 可能已被消耗（如果触发了 flush），但至少不应为负数
        assertTrue(coordinator.getPendingPacketCount() >= 0);
    }

    @Test
    public void testMetricsUpdatedAfterFlush() {
        coordinator.commitOffset("sink-1", 5000L);
        coordinator.flush();

        assertEquals(5000L, metricsCollector.getConfirmedOffset());
        assertFalse(metricsCollector.getLastCheckpointFlushTime().isEmpty());
    }
}
