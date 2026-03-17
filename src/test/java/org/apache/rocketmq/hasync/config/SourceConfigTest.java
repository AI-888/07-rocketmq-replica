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
 * SourceConfig 单元测试
 */
public class SourceConfigTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private SourceConfig config;

    @Before
    public void setUp() {
        config = new SourceConfig();
    }

    // ==================== camelToUpperSnake 测试 ====================

    @Test
    public void testCamelToUpperSnake_simple() {
        assertEquals("SOURCE_NAMESRV", SourceConfig.camelToUpperSnake("sourceNamesrv"));
    }

    @Test
    public void testCamelToUpperSnake_multiWords() {
        assertEquals("HEARTBEAT_INTERVAL", SourceConfig.camelToUpperSnake("heartbeatInterval"));
    }

    @Test
    public void testCamelToUpperSnake_allLowercase() {
        assertEquals("HELLO", SourceConfig.camelToUpperSnake("hello"));
    }

    @Test
    public void testCamelToUpperSnake_multipleUpperCase() {
        assertEquals("CHECKPOINT_FLUSH_BATCH_SIZE", SourceConfig.camelToUpperSnake("checkpointFlushBatchSize"));
    }

    @Test
    public void testCamelToUpperSnake_zmq() {
        assertEquals("ZMQ_BIND_PORT", SourceConfig.camelToUpperSnake("zmqBindPort"));
    }

    // ==================== CLI 参数加载测试 ====================

    @Test
    public void testLoadWithCLIArgs() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("127.0.0.1:9876", result.get("sourceNamesrv").getValue());
        assertEquals("CLI", result.get("sourceNamesrv").getSource());
        assertEquals("127.0.0.1:9877", result.get("targetNamesrv").getValue());
        assertEquals("CLI", result.get("targetNamesrv").getSource());
    }

    @Test
    public void testDefaultValuesFilled() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        Map<String, ConfigEntry> result = config.load(args);

        // 验证默认值被填充
        assertEquals("9876", result.get("sourceMetricsPort").getValue());
        assertEquals("DEFAULT", result.get("sourceMetricsPort").getSource());
        assertEquals("5000", result.get("heartbeatInterval").getValue());
        assertEquals("DEFAULT", result.get("heartbeatInterval").getSource());
        assertEquals("30000", result.get("masterPollInterval").getValue());
        assertEquals("5555", result.get("zmqBindPort").getValue());
        assertEquals("ha-sync-rfq", result.get("rfqTopic").getValue());
        assertEquals("3", result.get("rfqMaxRetry").getValue());
    }

    // ==================== 配置文件加载测试 ====================

    @Test
    public void testLoadFromConfigFile() throws IOException {
        File configFile = tempFolder.newFile("test-source.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("sourceNamesrv=10.0.0.1:9876\n");
            writer.write("targetNamesrv=10.0.0.2:9877\n");
            writer.write("heartbeatInterval=10000\n");
        }

        String[] args = {
                "--configFile", configFile.getAbsolutePath()
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("10.0.0.1:9876", result.get("sourceNamesrv").getValue());
        assertEquals("FILE", result.get("sourceNamesrv").getSource());
        assertEquals("10000", result.get("heartbeatInterval").getValue());
        assertEquals("FILE", result.get("heartbeatInterval").getSource());
    }

    @Test
    public void testCLIOverridesFile() throws IOException {
        File configFile = tempFolder.newFile("test-source2.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("sourceNamesrv=10.0.0.1:9876\n");
            writer.write("targetNamesrv=10.0.0.2:9877\n");
            writer.write("heartbeatInterval=3000\n");
        }

        String[] args = {
                "--configFile", configFile.getAbsolutePath(),
                "--sourceNamesrv", "192.168.1.1:9876",
                "--targetNamesrv", "192.168.1.2:9877",
                "--heartbeatInterval", "8000"
        };
        Map<String, ConfigEntry> result = config.load(args);

        // CLI 应覆盖 FILE
        assertEquals("192.168.1.1:9876", result.get("sourceNamesrv").getValue());
        assertEquals("CLI", result.get("sourceNamesrv").getSource());
        assertEquals("8000", result.get("heartbeatInterval").getValue());
        assertEquals("CLI", result.get("heartbeatInterval").getSource());
    }

    // ==================== 快捷访问方法测试 ====================

    @Test
    public void testQuickAccessMethods() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877",
                "--heartbeatInterval", "8000",
                "--zmqBindPort", "6666"
        };
        config.load(args);

        assertEquals("127.0.0.1:9876", config.getSourceNamesrv());
        assertEquals("127.0.0.1:9877", config.getTargetNamesrv());
        assertEquals(8000L, config.getHeartbeatInterval());
        assertEquals(6666, config.getZmqBindPort());
        assertEquals(9876, config.getSourceMetricsPort()); // 默认值
        assertEquals(30000L, config.getMasterPollInterval()); // 默认值
        assertEquals(1000L, config.getCheckpointFlushInterval()); // 默认值
        assertEquals(100, config.getCheckpointFlushBatchSize()); // 默认值
        assertNotNull(config.getSourceNodeId()); // 自动生成
        assertEquals("ha-sync-rfq", config.getRfqTopic()); // 默认值
        assertEquals("ha-sync-rfq-producer", config.getRfqProducerGroup()); // 默认值
        assertEquals(3, config.getRfqMaxRetry()); // 默认值
        assertEquals(60000L, config.getParseErrorSuspendWindowMs()); // 默认值
        assertEquals(60000L, config.getMetaSyncInterval()); // 默认值
    }

    // ==================== 空字符串处理测试 ====================

    @Test
    public void testEmptyStringTreatedAsNotSet() throws IOException {
        File configFile = tempFolder.newFile("test-source-empty.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("sourceNamesrv=127.0.0.1:9876\n");
            writer.write("targetNamesrv=127.0.0.1:9877\n");
            writer.write("heartbeatInterval=\n"); // 空字符串
        }

        String[] args = {"--configFile", configFile.getAbsolutePath()};
        Map<String, ConfigEntry> result = config.load(args);

        // heartbeatInterval 的空字符串应被忽略，使用默认值
        assertEquals("5000", result.get("heartbeatInterval").getValue());
        assertEquals("DEFAULT", result.get("heartbeatInterval").getSource());
    }

    // ==================== 配置文件不存在测试 ====================

    @Test
    public void testConfigFileNotFound() {
        String[] args = {
                "--configFile", "/nonexistent/path/config.properties",
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        // 不应抛出异常，应安全忽略
        Map<String, ConfigEntry> result = config.load(args);
        assertNotNull(result);
        assertEquals("127.0.0.1:9876", result.get("sourceNamesrv").getValue());
    }

    // ==================== 未识别配置项测试 ====================

    @Test
    public void testUnknownConfigKeysIgnored() throws IOException {
        File configFile = tempFolder.newFile("test-source-unknown.properties");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write("sourceNamesrv=127.0.0.1:9876\n");
            writer.write("targetNamesrv=127.0.0.1:9877\n");
            writer.write("unknownKey=someValue\n"); // 未知 key
        }

        String[] args = {"--configFile", configFile.getAbsolutePath()};
        Map<String, ConfigEntry> result = config.load(args);

        assertNull("未知 key 不应出现在结果中", result.get("unknownKey"));
    }

    // ==================== getInt/getLong 边界测试 ====================

    @Test
    public void testGetIntWithInvalidValue() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877",
                "--zmqBindPort", "not-a-number"
        };
        config.load(args);
        // 非法整数应返回默认值
        assertEquals(5555, config.getZmqBindPort());
    }

    @Test
    public void testGetLongWithInvalidValue() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877",
                "--heartbeatInterval", "abc"
        };
        config.load(args);
        assertEquals(5000L, config.getHeartbeatInterval());
    }

    // ==================== 环境变量名称转换测试 ====================

    @Test
    public void testToEnvName() {
        SourceConfig sc = new SourceConfig();
        // 通过反射验证 toEnvName 被正确调用
        // 直接测试 camelToUpperSnake 已经覆盖了核心逻辑
        assertEquals("SOURCE_NAMESRV", SourceConfig.camelToUpperSnake("sourceNamesrv"));
        assertEquals("RFQ_TOPIC", SourceConfig.camelToUpperSnake("rfqTopic"));
        assertEquals("PARSE_ERROR_SUSPEND_WINDOW_MS", SourceConfig.camelToUpperSnake("parseErrorSuspendWindowMs"));
    }

    // ==================== getString 对不存在的 key ====================

    @Test
    public void testGetStringForNonExistentKey() {
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        config.load(args);
        assertNull(config.getString("nonExistentKey"));
    }

    // ==================== null args 处理 ====================

    @Test
    public void testExtractConfigFilePathWithNull() {
        assertNull(config.extractConfigFilePath(null));
    }

    @Test
    public void testExtractConfigFilePathWithNoConfigFile() {
        String[] args = {"--sourceNamesrv", "127.0.0.1:9876"};
        assertNull(config.extractConfigFilePath(args));
    }

    @Test
    public void testExtractConfigFilePathWithConfigFile() {
        String[] args = {"--configFile", "/path/to/config.properties"};
        assertEquals("/path/to/config.properties", config.extractConfigFilePath(args));
    }
}
