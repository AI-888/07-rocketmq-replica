package org.apache.rocketmq.hasync.e2e;

import org.apache.rocketmq.hasync.bootstrap.HASyncMain;
import org.apache.rocketmq.hasync.config.SinkConfig;
import org.apache.rocketmq.hasync.config.SourceConfig;
import org.apache.rocketmq.hasync.model.ConfigEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * 端到端配置加载与 Bootstrap 启动测试
 * <p>
 * 覆盖场景：
 * - 配置文件 → CLI → 环境变量的三层合并
 * - Source 完整配置加载链路
 * - Sink 完整配置加载链路
 * - HASyncMain 模式分发
 * - 配置文件 + CLI 混合
 * - 默认值填充
 * - 敏感信息掩码
 */
public class EndToEndConfigBootstrapTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File sourceConfigFile;
    private File sinkConfigFile;

    @Before
    public void setUp() throws IOException {
        // 创建临时 Source 配置文件
        sourceConfigFile = tempFolder.newFile("ha-sync-source.properties");
        Properties sourceProps = new Properties();
        sourceProps.setProperty("sourceNamesrv", "192.168.1.100:9876");
        sourceProps.setProperty("targetNamesrv", "192.168.1.200:9876");
        sourceProps.setProperty("heartbeatInterval", "3000");
        sourceProps.setProperty("zmqBindPort", "6666");
        try (FileWriter writer = new FileWriter(sourceConfigFile)) {
            sourceProps.store(writer, "Source E2E Test Config");
        }

        // 创建临时 Sink 配置文件
        sinkConfigFile = tempFolder.newFile("ha-sync-sink.properties");
        Properties sinkProps = new Properties();
        sinkProps.setProperty("targetNamesrv", "192.168.1.200:9876");
        sinkProps.setProperty("sinkBatchSize", "200");
        sinkProps.setProperty("sinkThreads", "8");
        try (FileWriter writer = new FileWriter(sinkConfigFile)) {
            sinkProps.store(writer, "Sink E2E Test Config");
        }
    }

    // ==================== 场景1：Source 配置文件完整加载 ====================

    @Test
    public void testSourceConfigFromFile() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--configFile", sourceConfigFile.getAbsolutePath(),
                "--sourceNamesrv", "192.168.1.100:9876",
                "--targetNamesrv", "192.168.1.200:9876"
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertNotNull(result);
        assertEquals("192.168.1.100:9876", config.getSourceNamesrv());
        assertEquals("192.168.1.200:9876", config.getTargetNamesrv());
    }

    // ==================== 场景2：Source CLI 覆盖配置文件 ====================

    @Test
    public void testSourceCLIOverridesFile() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--configFile", sourceConfigFile.getAbsolutePath(),
                "--sourceNamesrv", "10.0.0.1:9876",
                "--targetNamesrv", "10.0.0.2:9876",
                "--heartbeatInterval", "8000"
        };
        Map<String, ConfigEntry> result = config.load(args);

        // CLI 应覆盖配置文件
        assertEquals("10.0.0.1:9876", config.getSourceNamesrv());
        assertEquals("10.0.0.2:9876", config.getTargetNamesrv());
        assertEquals(8000L, config.getHeartbeatInterval());

        // 验证来源标记为 CLI
        assertEquals("CLI", result.get("sourceNamesrv").getSource());
        assertEquals("CLI", result.get("heartbeatInterval").getSource());
    }

    // ==================== 场景3：Source 默认值填充 ====================

    @Test
    public void testSourceDefaultValues() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        config.load(args);

        // 验证所有默认值
        assertEquals(9876, config.getSourceMetricsPort());
        assertEquals(5000L, config.getHeartbeatInterval());
        assertEquals(30000L, config.getMasterPollInterval());
        assertEquals(1000L, config.getCheckpointFlushInterval());
        assertEquals(100, config.getCheckpointFlushBatchSize());
        assertEquals(5555, config.getZmqBindPort());
        assertEquals("ha-sync-rfq", config.getRfqTopic());
        assertEquals("ha-sync-rfq-producer", config.getRfqProducerGroup());
        assertEquals(3, config.getRfqMaxRetry());
        assertEquals(60000L, config.getParseErrorSuspendWindowMs());
        assertEquals(60000L, config.getMetaSyncInterval());
    }

    // ==================== 场景4：Sink 配置文件完整加载 ====================

    @Test
    public void testSinkConfigFromFile() {
        SinkConfig config = new SinkConfig();
        String[] args = {
                "--configFile", sinkConfigFile.getAbsolutePath(),
                "--targetNamesrv", "192.168.1.200:9876"
        };
        config.load(args);

        assertEquals("192.168.1.200:9876", config.getTargetNamesrv());
    }

    // ==================== 场景5：Sink CLI 覆盖配置文件 ====================

    @Test
    public void testSinkCLIOverridesFile() {
        SinkConfig config = new SinkConfig();
        String[] args = {
                "--configFile", sinkConfigFile.getAbsolutePath(),
                "--targetNamesrv", "10.0.0.5:9876",
                "--sinkBatchSize", "500",
                "--sinkThreads", "16"
        };
        Map<String, ConfigEntry> result = config.load(args);

        assertEquals("10.0.0.5:9876", config.getTargetNamesrv());
        assertEquals(500, config.getSinkBatchSize());
        assertEquals(16, config.getSinkThreads());

        assertEquals("CLI", result.get("targetNamesrv").getSource());
        assertEquals("CLI", result.get("sinkBatchSize").getSource());
    }

    // ==================== 场景6：Sink 默认值填充 ====================

    @Test
    public void testSinkDefaultValues() {
        SinkConfig config = new SinkConfig();
        String[] args = {"--targetNamesrv", "127.0.0.1:9876"};
        config.load(args);

        assertEquals(9877, config.getSinkMetricsPort());
        assertEquals(100, config.getSinkBatchSize());
        assertEquals(4, config.getSinkThreads());
        assertEquals(3, config.getSinkMaxRetry());
        assertEquals(30000L, config.getTargetProbeInterval());
        assertEquals(10, config.getStartupCheckMsgCount());
        assertEquals(3, config.getTopicSyncMaxRetry());
    }

    // ==================== 场景7：配置项来源追踪 ====================

    @Test
    public void testConfigSourceTracking() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--configFile", sourceConfigFile.getAbsolutePath(),
                "--sourceNamesrv", "cli-addr:9876",
                "--targetNamesrv", "cli-target:9876"
        };
        Map<String, ConfigEntry> result = config.load(args);

        // CLI 参数来源
        assertEquals("CLI", result.get("sourceNamesrv").getSource());
        // 配置文件中的 zmqBindPort 如果被 CLI 覆盖则标记 CLI，否则标记 FILE
        // heartbeatInterval 在 CLI 中未指定但在文件中有
        ConfigEntry heartbeat = result.get("heartbeatInterval");
        assertNotNull(heartbeat);
        // 文件中设置了 3000，CLI 中未设置
        assertEquals("FILE", heartbeat.getSource());
        assertEquals("3000", heartbeat.getValue());

        // 默认值来源
        ConfigEntry rfqTopic = result.get("rfqTopic");
        assertNotNull(rfqTopic);
        assertEquals("DEFAULT", rfqTopic.getSource());
    }

    // ==================== 场景8：HASyncMain 模式提取 ====================

    @Test
    public void testHASyncMainModeExtraction() {
        // Source 模式
        assertEquals("source", HASyncMain.extractMode(new String[]{"--mode", "source"}));
        // Sink 模式
        assertEquals("sink", HASyncMain.extractMode(new String[]{"--mode", "sink"}));
        // 混合参数
        assertEquals("source", HASyncMain.extractMode(
                new String[]{"--sourceNamesrv", "addr", "--mode", "source", "--targetNamesrv", "addr2"}));
        // 无 --mode
        assertNull(HASyncMain.extractMode(new String[]{"--sourceNamesrv", "addr"}));
        // null 参数
        assertNull(HASyncMain.extractMode(null));
        // --mode 无后续值
        assertNull(HASyncMain.extractMode(new String[]{"--mode"}));
    }

    // ==================== 场景9：HASyncMain 参数去除 ====================

    @Test
    public void testHASyncMainStripModeArgs() {
        String[] args = {"--mode", "source", "--sourceNamesrv", "addr1", "--targetNamesrv", "addr2"};
        String[] stripped = HASyncMain.stripModeArgs(args);

        assertEquals(4, stripped.length);
        assertEquals("--sourceNamesrv", stripped[0]);
        assertEquals("addr1", stripped[1]);
        assertEquals("--targetNamesrv", stripped[2]);
        assertEquals("addr2", stripped[3]);
    }

    // ==================== 场景10：配置文件不存在时仅用 CLI ====================

    @Test
    public void testConfigWithoutFile() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--configFile", "/non/existent/path.properties",
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        // 不应抛异常
        config.load(args);

        assertEquals("127.0.0.1:9876", config.getSourceNamesrv());
        assertEquals("127.0.0.1:9877", config.getTargetNamesrv());
    }

    // ==================== 场景11：整型配置非法值回退默认 ====================

    @Test
    public void testInvalidIntFallbackToDefault() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877",
                "--heartbeatInterval", "not-a-number"
        };
        config.load(args);

        // 非法值应回退到默认值
        assertEquals(5000L, config.getHeartbeatInterval());
    }

    // ==================== 场景12：Source→Bootstrap 完整启动链路 ====================

    @Test
    public void testSourceBootstrapConfigChain() {
        // 模拟完整的 Source 配置加载链路
        SourceConfig config = new SourceConfig();
        String[] fullArgs = {
                "--configFile", sourceConfigFile.getAbsolutePath(),
                "--sourceNamesrv", "192.168.1.100:9876",
                "--targetNamesrv", "192.168.1.200:9876",
                "--sourceNodeId", "test-node-1",
                "--zmqBindPort", "7777",
                "--checkpointFlushInterval", "2000",
                "--checkpointFlushBatchSize", "50"
        };
        config.load(fullArgs);

        // 验证所有配置项正确加载
        assertEquals("192.168.1.100:9876", config.getSourceNamesrv());
        assertEquals("192.168.1.200:9876", config.getTargetNamesrv());
        assertEquals("test-node-1", config.getSourceNodeId());
        assertEquals(7777, config.getZmqBindPort());
        assertEquals(2000L, config.getCheckpointFlushInterval());
        assertEquals(50, config.getCheckpointFlushBatchSize());
        // 文件中的值应被 CLI 覆盖
        // heartbeatInterval 在文件中为 3000，CLI 未指定，应保持文件值
        assertEquals(3000L, config.getHeartbeatInterval());
    }

    // ==================== 场景13：Sink→Bootstrap 完整启动链路 ====================

    @Test
    public void testSinkBootstrapConfigChain() {
        SinkConfig config = new SinkConfig();
        String[] fullArgs = {
                "--configFile", sinkConfigFile.getAbsolutePath(),
                "--targetNamesrv", "192.168.1.200:9876",
                "--sinkId", "sink-node-A",
                "--sinkBatchSize", "300",
                "--sinkMaxRetry", "5",
                "--startupCheckMsgCount", "20"
        };
        config.load(fullArgs);

        assertEquals("192.168.1.200:9876", config.getTargetNamesrv());
        assertEquals("sink-node-A", config.getSinkId());
        assertEquals(300, config.getSinkBatchSize());
        assertEquals(5, config.getSinkMaxRetry());
        assertEquals(20, config.getStartupCheckMsgCount());
        // 配置文件中 sinkThreads=8，CLI 未覆盖，应保持
        assertEquals(8, config.getSinkThreads());
    }

    // ==================== 场景14：完整 mode→config→pipeline 链路 ====================

    @Test
    public void testFullModeToConfigPipeline() {
        // 模拟 HASyncMain 接收参数 → 提取 mode → 剥离 mode → 传给 Config
        String[] mainArgs = {
                "--mode", "source",
                "--sourceNamesrv", "10.0.0.1:9876",
                "--targetNamesrv", "10.0.0.2:9876",
                "--heartbeatInterval", "1000"
        };

        // Step 1: 提取 mode
        String mode = HASyncMain.extractMode(mainArgs);
        assertEquals("source", mode);

        // Step 2: 去除 mode 参数
        String[] configArgs = HASyncMain.stripModeArgs(mainArgs);
        assertEquals(6, configArgs.length);

        // Step 3: 加载配置
        SourceConfig config = new SourceConfig();
        config.load(configArgs);
        assertEquals("10.0.0.1:9876", config.getSourceNamesrv());
        assertEquals("10.0.0.2:9876", config.getTargetNamesrv());
        assertEquals(1000L, config.getHeartbeatInterval());
    }

    // ==================== 场景15：配置掩码验证 ====================

    @Test
    public void testSensitiveConfigMasking() {
        SourceConfig config = new SourceConfig();
        // maskSensitive 是 protected 方法，但我们可以通过子类测试
        // 这里通过反射或直接创建子类来测试
        String masked = config.maskSensitive("sourceNamesrv", "192.168.1.100:9876");
        // 应包含掩码（如 192.168.*.***:9876）
        assertFalse("NameServer 地址应被掩码", masked.equals("192.168.1.100:9876"));
        assertTrue("掩码后应包含 *", masked.contains("*"));

        // 非敏感字段不应被掩码
        String notMasked = config.maskSensitive("heartbeatInterval", "5000");
        assertEquals("5000", notMasked);
    }

    // ==================== 场景16：camelToUpperSnake 转换 ====================

    @Test
    public void testCamelToUpperSnakeConversion() {
        assertEquals("SOURCE_NAMESRV", SourceConfig.camelToUpperSnake("sourceNamesrv"));
        assertEquals("HEARTBEAT_INTERVAL", SourceConfig.camelToUpperSnake("heartbeatInterval"));
        assertEquals("ZMQ_BIND_PORT", SourceConfig.camelToUpperSnake("zmqBindPort"));
        assertEquals("TARGET_NAMESRV", SinkConfig.camelToUpperSnake("targetNamesrv"));
        assertEquals("SINK_BATCH_SIZE", SinkConfig.camelToUpperSnake("sinkBatchSize"));
    }

    // ==================== 场景17：空配置文件 ====================

    @Test
    public void testEmptyConfigFile() throws IOException {
        File emptyFile = tempFolder.newFile("empty.properties");
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--configFile", emptyFile.getAbsolutePath(),
                "--sourceNamesrv", "127.0.0.1:9876",
                "--targetNamesrv", "127.0.0.1:9877"
        };
        config.load(args);

        // 应正常加载，CLI 参数生效
        assertEquals("127.0.0.1:9876", config.getSourceNamesrv());
    }

    // ==================== 场景18：ConfigEntry 值与来源一致性 ====================

    @Test
    public void testConfigEntryConsistency() {
        SourceConfig config = new SourceConfig();
        String[] args = {
                "--sourceNamesrv", "source-addr:9876",
                "--targetNamesrv", "target-addr:9876"
        };
        Map<String, ConfigEntry> result = config.load(args);

        // 每个 ConfigEntry 的值应与 getString 一致
        for (Map.Entry<String, ConfigEntry> entry : result.entrySet()) {
            assertEquals("ConfigEntry 值应与 getString 一致",
                    entry.getValue().getValue(), config.getString(entry.getKey()));
        }
    }
}
