package org.apache.rocketmq.hasync.reliability;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * SnapshotWriter 单元测试
 */
public class SnapshotWriterTest {

    private static final String TEST_DIR = "target/test-snapshots";
    private SnapshotWriter writer;
    private MetricsCollector metricsCollector;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
        writer = new SnapshotWriter(TEST_DIR, metricsCollector);
    }

    @After
    public void tearDown() {
        // 清理测试文件
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
            dir.delete();
        }
    }

    @Test
    public void testWriteSnapshot() {
        writer.setDataProvider(new SnapshotWriter.SnapshotDataProvider() {
            @Override
            public long getConfirmedOffset() { return 12345L; }
            @Override
            public String getMasterAddr() { return "192.168.1.100:10912"; }
            @Override
            public Map<String, Long> getTopicBytesStats() { return null; }
        });

        writer.writeSnapshot();

        File snapshotFile = new File(TEST_DIR, "snapshot.json");
        assertTrue(snapshotFile.exists());
        assertTrue(snapshotFile.length() > 0);
    }

    @Test
    public void testReadSnapshot() {
        writer.setDataProvider(new SnapshotWriter.SnapshotDataProvider() {
            @Override
            public long getConfirmedOffset() { return 99999L; }
            @Override
            public String getMasterAddr() { return "10.0.0.1:10912"; }
            @Override
            public Map<String, Long> getTopicBytesStats() { return null; }
        });

        writer.writeSnapshot();
        Map<String, Object> data = writer.readSnapshot();

        assertNotNull(data);
        assertNotNull(data.get("snapshotTime"));
    }

    @Test
    public void testReadSnapshotNotExists() {
        Map<String, Object> data = writer.readSnapshot();
        assertNull(data);
    }

    @Test
    public void testWriteWithNoDataProvider() {
        writer.writeSnapshot();

        File snapshotFile = new File(TEST_DIR, "snapshot.json");
        assertTrue(snapshotFile.exists());

        Map<String, Object> data = writer.readSnapshot();
        assertNotNull(data);
        assertNotNull(data.get("snapshotTime"));
    }

    @Test
    public void testAtomicWrite() {
        writer.setDataProvider(new SnapshotWriter.SnapshotDataProvider() {
            @Override
            public long getConfirmedOffset() { return 100L; }
            @Override
            public String getMasterAddr() { return "localhost"; }
            @Override
            public Map<String, Long> getTopicBytesStats() { return null; }
        });

        writer.writeSnapshot();

        // 确认没有残留的临时文件
        File tmpFile = new File(TEST_DIR, "snapshot.json.tmp");
        assertFalse(tmpFile.exists());
    }

    @Test
    public void testGetSnapshotFilePath() {
        String path = writer.getSnapshotFilePath();
        assertTrue(path.endsWith("snapshot.json"));
        assertTrue(path.contains(TEST_DIR));
    }

    @Test
    public void testMultipleWrites() {
        writer.setDataProvider(new SnapshotWriter.SnapshotDataProvider() {
            @Override
            public long getConfirmedOffset() { return 100L; }
            @Override
            public String getMasterAddr() { return "host1"; }
            @Override
            public Map<String, Long> getTopicBytesStats() { return null; }
        });

        writer.writeSnapshot();
        writer.writeSnapshot();
        writer.writeSnapshot();

        // 应该只有一个快照文件（后续覆盖前面的）
        File dir = new File(TEST_DIR);
        File[] files = dir.listFiles((d, name) -> name.equals("snapshot.json"));
        assertNotNull(files);
        assertEquals(1, files.length);
    }
}
