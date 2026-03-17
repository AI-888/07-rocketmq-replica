package org.apache.rocketmq.hasync.config;

import org.apache.rocketmq.hasync.model.ConfigEntry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * SinkConfig 单元测试
 */
public class SinkConfigTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private SinkConfig config;

    @Before
    public void setUp() {
        config = new SinkConfig();
    }

    // ==================== camelToUpperSnake 测试 ====================

    @Test
    public void testCamelToUpperSnake_targetNamesrv() {
        assertEquals("TARGET_NAMESRV", SinkConfig.camelToUpperSnake("targetNamesrv"));
    }

    @Test
    public void testCamelToUpperSnake_sinkBatchSize() {
        assertEquals("SINK_BATCH_SIZE", SinkConfig.camelToUpperSnake("sinkBatchSize"));
    }

    @Test
    public void testCamelToUpperSnake_startupCheckMsgCount() {
        assertEquals("STARTUP_CHECK_MSG_COUNT", SinkConfig.camelToUpperSnake("startupCheckMsgCount"));
    }

    // ==================== CLI 参数加载测试 ====================

    @Test
    public void testLoadWithCLIArgs() {
        String[] args = {
                "--targetNamesrv", "127.0.0.1:9877"
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("127.0.0.1:9877", result.get("targetNamesrv").getValue());
        assertEquals("CLI", result.get("targetNamesrv").getSource());
    }

    @Test
    public void testDefaultValuesFilled() {
        String[] args = {"--targetNamesrv", "127.0.0.1:9877"};
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("9877", result.get("sinkMetricsPort").getValue());
        assertEquals("DEFAULT", result.get("sinkMetricsPort").getSource());
        assertEquals("100", result.get("sinkBatchSize").getValue());
        assertEquals("4", result.get("sinkThreads").getValue());
        assertEquals("3", result.get("sinkMaxRetry").getValue());
        assertEquals("30000", result.get("targetProbeInterval").getValue());
        assertEquals("10", result.get("startupCheckMsgCount").getValue());
        assertEquals("3", result.get("topicSyncMaxRetry").getValue());
        assertNotNull(result.get("sinkId"));
    }

    // ==================== 配置文件加载测试 ====================

    @Test
    public void testLoadFromConfigFile() throws IOException {
        File configFile = tempFolder.newFile("test-sink.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("targetNamesrv=10.0.0.1:9877\n");
            writer.write("sinkBatchSize=200\n");
            writer.write("sinkThreads=8\n");
        }

        String[] args = {"--configFile", configFile.getAbsolutePath()};
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("10.0.0.1:9877", result.get("targetNamesrv").getValue());
        assertEquals("FILE", result.get("targetNamesrv").getSource());
        assertEquals("200", result.get("sinkBatchSize").getValue());
        assertEquals("8", result.get("sinkThreads").getValue());
    }

    @Test
    public void testCLIOverridesFile() throws IOException {
        File configFile = tempFolder.newFile("test-sink2.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("targetNamesrv=10.0.0.1:9877\n");
            writer.write("sinkBatchSize=200\n");
        }

        String[] args = {
                "--configFile", configFile.getAbsolutePath(),
                "--targetNamesrv", "192.168.1.1:9877",
                "--sinkBatchSize", "500"
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("192.168.1.1:9877", result.get("targetNamesrv").getValue());
        assertEquals("CLI", result.get("targetNamesrv").getSource());
        assertEquals("500", result.get("sinkBatchSize").getValue());
        assertEquals("CLI", result.get("sinkBatchSize").getSource());
    }

    // ==================== 快捷访问方法测试 ====================

    @Test
    public void testQuickAccessMethods() {
        String[] args = {
                "--targetNamesrv", "127.0.0.1:9877",
                "--sinkBatchSize", "300",
                "--sinkThreads", "16",
                "--sinkMaxRetry", "5"
        };
        config.load(args);

        assertEquals("127.0.0.1:9877", config.getTargetNamesrv());
        assertEquals(300, config.getSinkBatchSize());
        assertEquals(16, config.getSinkThreads());
        assertEquals(5, config.getSinkMaxRetry());
        assertEquals(9877, config.getSinkMetricsPort()); // 默认值
        assertNotNull(config.getSinkId()); // 自动生成
        assertEquals(30000L, config.getTargetProbeInterval()); // 默认值
        assertEquals(10, config.getStartupCheckMsgCount()); // 默认值
        assertEquals(3, config.getTopicSyncMaxRetry()); // 默认值
    }

    // ==================== 空字符串处理 ====================

    @Test
    public void testEmptyStringTreatedAsNotSet() throws IOException {
        File configFile = tempFolder.newFile("test-sink-empty.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("targetNamesrv=127.0.0.1:9877\n");
            writer.write("sinkBatchSize=\n");
        }

        String[] args = {"--configFile", configFile.getAbsolutePath()};
        config.load(args);

        // 空值应回退到默认值
        assertEquals(100, config.getSinkBatchSize());
    }

    // ==================== getInt 非法值测试 ====================

    @Test
    public void testGetIntWithInvalidValue() {
        String[] args = {
                "--targetNamesrv", "127.0.0.1:9877",
                "--sinkBatchSize", "xyz"
        };
        config.load(args);
        assertEquals(100, config.getSinkBatchSize()); // 返回默认值
    }

    // ==================== 配置文件不存在 ====================

    @Test
    public void testConfigFileNotFound() {
        String[] args = {
                "--configFile", "/nonexistent/path.properties",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        Map<String, ConfigEntry> result = config.load(args);
        assertNotNull(result);
        assertEquals("127.0.0.1:9877", result.get("targetNamesrv").getValue());
    }

    // ==================== 未识别 key 被忽略 ====================

    @Test
    public void testUnknownKeysIgnored() throws IOException {
        File configFile = tempFolder.newFile("test-sink-unknown.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("targetNamesrv=127.0.0.1:9877\n");
            writer.write("fooBar=baz\n");
        }

        String[] args = {"--configFile", configFile.getAbsolutePath()};
        Map<String, ConfigEntry> result = config.load(args);

        assertNull(result.get("fooBar"));
    }
}
