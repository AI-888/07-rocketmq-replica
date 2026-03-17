package org.apache.rocketmq.hasync.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Source 进程配置 — ha-sync-source 的启动参数
 * <p>
 * 对应需求 1 §1-2（Source 必填/可选参数）
 *
 * @see AbstractConfig
 */
public class SourceConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(SourceConfig.class);

    /** 环境变量前缀 */
    private static final String ENV_PREFIX = "HA_SOURCE_";

    // ==================== 配置项 Key 常量 ====================

    public static final String SOURCE_NAMESRV = "sourceNamesrv";
    public static final String TARGET_NAMESRV = "targetNamesrv";
    public static final String SOURCE_METRICS_PORT = "sourceMetricsPort";
    public static final String HEARTBEAT_INTERVAL = "heartbeatInterval";
    public static final String MASTER_POLL_INTERVAL = "masterPollInterval";
    public static final String CHECKPOINT_FLUSH_INTERVAL = "checkpointFlushInterval";
    public static final String CHECKPOINT_FLUSH_BATCH_SIZE = "checkpointFlushBatchSize";
    public static final String SOURCE_NODE_ID = "sourceNodeId";
    public static final String ZMQ_BIND_PORT = "zmqBindPort";
    public static final String RFQ_TOPIC = "rfqTopic";
    public static final String RFQ_PRODUCER_GROUP = "rfqProducerGroup";
    public static final String RFQ_MAX_RETRY = "rfqMaxRetry";
    public static final String PARSE_ERROR_SUSPEND_WINDOW_MS = "parseErrorSuspendWindowMs";
    public static final String META_SYNC_INTERVAL = "metaSyncInterval";
    public static final String CONFIG_FILE = "configFile";
    public static final String BROKER_NAME = "brokerName";

    /** 所有配置项 key（有序） */
    private static final Set<String> ALL_KEYS = new LinkedHashSet<>();

    /** 必填配置项 key */
    private static final Set<String> REQUIRED_KEYS = new HashSet<>();

    /** 默认值映射 */
    private static final Map<String, String> DEFAULTS = new HashMap<>();

    static {
        // 注册所有配置项
        ALL_KEYS.add(SOURCE_NAMESRV);
        ALL_KEYS.add(TARGET_NAMESRV);
        ALL_KEYS.add(SOURCE_METRICS_PORT);
        ALL_KEYS.add(HEARTBEAT_INTERVAL);
        ALL_KEYS.add(MASTER_POLL_INTERVAL);
        ALL_KEYS.add(CHECKPOINT_FLUSH_INTERVAL);
        ALL_KEYS.add(CHECKPOINT_FLUSH_BATCH_SIZE);
        ALL_KEYS.add(SOURCE_NODE_ID);
        ALL_KEYS.add(ZMQ_BIND_PORT);
        ALL_KEYS.add(RFQ_TOPIC);
        ALL_KEYS.add(RFQ_PRODUCER_GROUP);
        ALL_KEYS.add(RFQ_MAX_RETRY);
        ALL_KEYS.add(PARSE_ERROR_SUSPEND_WINDOW_MS);
        ALL_KEYS.add(META_SYNC_INTERVAL);
        ALL_KEYS.add(CONFIG_FILE);
        ALL_KEYS.add(BROKER_NAME);

        // 必填参数（需求 1 §1）
        REQUIRED_KEYS.add(SOURCE_NAMESRV);
        REQUIRED_KEYS.add(TARGET_NAMESRV);

        // 默认值（需求 1 §2）
        DEFAULTS.put(SOURCE_METRICS_PORT, "9876");
        DEFAULTS.put(HEARTBEAT_INTERVAL, "5000");
        DEFAULTS.put(MASTER_POLL_INTERVAL, "30000");
        DEFAULTS.put(CHECKPOINT_FLUSH_INTERVAL, "1000");
        DEFAULTS.put(CHECKPOINT_FLUSH_BATCH_SIZE, "100");
        DEFAULTS.put(SOURCE_NODE_ID, getDefaultNodeId());
        DEFAULTS.put(ZMQ_BIND_PORT, "5555");
        DEFAULTS.put(RFQ_TOPIC, "ha-sync-rfq");
        DEFAULTS.put(RFQ_PRODUCER_GROUP, "ha-sync-rfq-producer");
        DEFAULTS.put(RFQ_MAX_RETRY, "3");
        DEFAULTS.put(PARSE_ERROR_SUSPEND_WINDOW_MS, "60000");
        DEFAULTS.put(META_SYNC_INTERVAL, "60000");
        DEFAULTS.put(BROKER_NAME, "broker-a");
    }

    // ==================== 快捷访问方法 ====================

    public String getSourceNamesrv() {
        return getString(SOURCE_NAMESRV);
    }

    public String getTargetNamesrv() {
        return getString(TARGET_NAMESRV);
    }

    public int getSourceMetricsPort() {
        return getInt(SOURCE_METRICS_PORT, 9876);
    }

    public long getHeartbeatInterval() {
        return getLong(HEARTBEAT_INTERVAL, 5000L);
    }

    public long getMasterPollInterval() {
        return getLong(MASTER_POLL_INTERVAL, 30000L);
    }

    public long getCheckpointFlushInterval() {
        return getLong(CHECKPOINT_FLUSH_INTERVAL, 1000L);
    }

    public int getCheckpointFlushBatchSize() {
        return getInt(CHECKPOINT_FLUSH_BATCH_SIZE, 100);
    }

    public String getSourceNodeId() {
        return getString(SOURCE_NODE_ID);
    }

    public int getZmqBindPort() {
        return getInt(ZMQ_BIND_PORT, 5555);
    }

    public String getRfqTopic() {
        return getString(RFQ_TOPIC);
    }

    public String getRfqProducerGroup() {
        return getString(RFQ_PRODUCER_GROUP);
    }

    public int getRfqMaxRetry() {
        return getInt(RFQ_MAX_RETRY, 3);
    }

    public long getParseErrorSuspendWindowMs() {
        return getLong(PARSE_ERROR_SUSPEND_WINDOW_MS, 60000L);
    }

    public long getMetaSyncInterval() {
        return getLong(META_SYNC_INTERVAL, 60000L);
    }

    // ==================== 抽象方法实现 ====================

    @Override
    protected Set<String> getAllConfigKeys() {
        return ALL_KEYS;
    }

    @Override
    protected Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @Override
    protected String getDefaultValue(String key) {
        return DEFAULTS.get(key);
    }

    /**
     * 环境变量命名规则（需求 1 §9）：
     * sourceNamesrv → HA_SOURCE_SOURCE_NAMESRV
     * heartbeatInterval → HA_SOURCE_HEARTBEAT_INTERVAL
     */
    @Override
    protected String toEnvName(String key) {
        return ENV_PREFIX + camelToUpperSnake(key);
    }

    @Override
    protected String getDefaultConfigFilePath() {
        return "./ha-sync-source.properties";
    }

    @Override
    protected void printUsage() {
        System.err.println("用法: java -jar ha-sync.jar --mode source [选项]");
        System.err.println();
        System.err.println("必填参数:");
        System.err.println("  --sourceNamesrv <addr>    源集群 NameServer 地址（多个以 ; 分隔）");
        System.err.println("  --targetNamesrv <addr>    目标集群 NameServer 地址");
        System.err.println();
        System.err.println("可选参数:");
        System.err.println("  --sourceMetricsPort <port>            HTTP 监控端口（默认: 9876）");
        System.err.println("  --heartbeatInterval <ms>              心跳间隔（默认: 5000）");
        System.err.println("  --masterPollInterval <ms>             Master 轮询间隔（默认: 30000）");
        System.err.println("  --checkpointFlushInterval <ms>        Checkpoint 刷写间隔（默认: 1000）");
        System.err.println("  --checkpointFlushBatchSize <n>        Checkpoint 批量刷写阈值（默认: 100）");
        System.err.println("  --sourceNodeId <id>                   节点标识（默认: hostname:pid）");
        System.err.println("  --zmqBindPort <port>                  ZMQ 绑定端口（默认: 5555）");
        System.err.println("  --rfqTopic <topic>                    RFQ Topic（默认: ha-sync-rfq）");
        System.err.println("  --rfqProducerGroup <group>            RFQ Producer Group（默认: ha-sync-rfq-producer）");
        System.err.println("  --rfqMaxRetry <n>                     RFQ 重试次数（默认: 3）");
        System.err.println("  --parseErrorSuspendWindowMs <ms>      解析错误暂停窗口（默认: 60000）");
        System.err.println("  --metaSyncInterval <ms>               元数据同步间隔（默认: 60000）");
        System.err.println("  --configFile <path>                   配置文件路径（默认: ./ha-sync-source.properties）");
    }

    // ==================== 工具方法 ====================

    /**
     * camelCase → UPPER_SNAKE_CASE
     * 例：sourceNamesrv → SOURCE_NAMESRV
     *     heartbeatInterval → HEARTBEAT_INTERVAL
     */
    public static String camelToUpperSnake(String camelCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                if (sb.length() > 0) {
                    sb.append('_');
                }
                sb.append(c);
            } else {
                sb.append(Character.toUpperCase(c));
            }
        }
        return sb.toString();
    }

    /**
     * 生成默认节点 ID：hostname:pid
     */
    private static String getDefaultNodeId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            return hostname + ":" + pid;
        } catch (Exception e) {
            return "unknown:" + System.currentTimeMillis();
        }
    }
}
