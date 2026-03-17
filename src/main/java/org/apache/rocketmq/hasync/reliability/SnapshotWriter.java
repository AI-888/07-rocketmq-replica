package org.apache.rocketmq.hasync.reliability;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 快照文件写入器
 * <p>
 * 对应需求 17 §3-5：
 * <ul>
 *   <li>优雅停机时写入 snapshot.json</li>
 *   <li>先写临时文件再原子重命名（需求 17 §5）</li>
 *   <li>可配置定期快照（默认 60 秒）</li>
 * </ul>
 * <p>
 * 快照内容：
 * <ul>
 *   <li>confirmedOffset</li>
 *   <li>masterAddr</li>
 *   <li>snapshotTime（ISO-8601）</li>
 *   <li>topicBytesStats</li>
 *   <li>syncSuccessCount / syncFailureCount / parseErrorCount</li>
 * </ul>
 */
public class SnapshotWriter {

    private static final Logger log = LoggerFactory.getLogger(SnapshotWriter.class);

    /** 默认快照文件名 */
    private static final String SNAPSHOT_FILE = "snapshot.json";
    /** 临时文件后缀 */
    private static final String TEMP_SUFFIX = ".tmp";

    private final String snapshotDir;
    private final MetricsCollector metricsCollector;

    /** 额外的快照数据提供者 */
    private SnapshotDataProvider dataProvider;

    /**
     * 快照数据提供者接口
     */
    public interface SnapshotDataProvider {
        long getConfirmedOffset();
        String getMasterAddr();
        Map<String, Long> getTopicBytesStats();
    }

    public SnapshotWriter(String snapshotDir, MetricsCollector metricsCollector) {
        this.snapshotDir = snapshotDir != null ? snapshotDir : ".";
        this.metricsCollector = metricsCollector;
    }

    public void setDataProvider(SnapshotDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    /**
     * 写入快照文件
     * <p>
     * 先写临时文件，再原子重命名（需求 17 §5）
     */
    public void writeSnapshot() {
        Map<String, Object> snapshot = buildSnapshot();

        File dir = new File(snapshotDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File tmpFile = new File(dir, SNAPSHOT_FILE + TEMP_SUFFIX);
        File targetFile = new File(dir, SNAPSHOT_FILE);

        try {
            // 写入临时文件
            try (FileOutputStream fos = new FileOutputStream(tmpFile);
                 OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
                String jsonStr = JSON.toJSONString(snapshot, JSONWriter.Feature.PrettyFormat);
                writer.write(jsonStr);
                writer.flush();
                fos.getFD().sync();
            }

            // 原子重命名
            Files.move(tmpFile.toPath(), targetFile.toPath(), StandardCopyOption.ATOMIC_MOVE);

            log.info("快照已写入: {}", targetFile.getAbsolutePath());

        } catch (Exception e) {
            log.error("快照写入失败: {}", e.getMessage(), e);
            // 清理临时文件
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    /**
     * 构建快照数据
     */
    private Map<String, Object> buildSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();

        // 基础信息
        if (dataProvider != null) {
            snapshot.put("confirmedOffset", dataProvider.getConfirmedOffset());
            snapshot.put("masterAddr", dataProvider.getMasterAddr());
            Map<String, Long> topicStats = dataProvider.getTopicBytesStats();
            if (topicStats != null) {
                snapshot.put("topicBytesStats", topicStats);
            }
        }

        snapshot.put("snapshotTime", Instant.now().toString());

        // 监控指标快照
        if (metricsCollector != null) {
            Map<String, Object> allMetrics = metricsCollector.getAllMetrics();
            if (allMetrics.containsKey("source")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceMetrics = (Map<String, Object>) allMetrics.get("source");
                if (sourceMetrics != null) {
                    snapshot.put("parseErrorCount", sourceMetrics.getOrDefault("parseErrorCount", 0L));
                }
            }
            if (allMetrics.containsKey("sink")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> sinkMetrics = (Map<String, Object>) allMetrics.get("sink");
                if (sinkMetrics != null) {
                    snapshot.put("syncSuccessCount", sinkMetrics.getOrDefault("syncSuccessCount", 0L));
                    snapshot.put("syncFailureCount", sinkMetrics.getOrDefault("syncFailureCount", 0L));
                }
            }
        }

        return snapshot;
    }

    /**
     * 读取快照文件
     */
    public Map<String, Object> readSnapshot() {
        File file = new File(snapshotDir, SNAPSHOT_FILE);
        if (!file.exists()) {
            return null;
        }

        try {
            byte[] bytes = Files.readAllBytes(file.toPath());
            @SuppressWarnings("unchecked")
            Map<String, Object> result = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8), Map.class);
            return result;
        } catch (Exception e) {
            log.error("读取快照文件失败: {}", e.getMessage());
            return null;
        }
    }

    public String getSnapshotFilePath() {
        return new File(snapshotDir, SNAPSHOT_FILE).getAbsolutePath();
    }
}
