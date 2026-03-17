package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Topic 白名单过滤器
 * <p>
 * 对应需求 11：Sink 端按白名单过滤消息，跳过不需要的 Topic。
 * <ul>
 *   <li>白名单为空 → 不过滤，所有 Topic 都通过</li>
 *   <li>白名单非空 → 仅匹配的 Topic 通过，其余跳过</li>
 *   <li>被过滤的消息仍推进 confirmedOffset</li>
 * </ul>
 */
public class TopicFilter {

    private static final Logger log = LoggerFactory.getLogger(TopicFilter.class);

    /** Topic 白名单（线程安全） */
    private final Set<String> whitelist;
    private MetricsCollector metricsCollector;

    /**
     * 创建 Topic 过滤器
     *
     * @param topics 白名单 Topic 列表（null 或空表示不过滤）
     */
    public TopicFilter(Set<String> topics) {
        if (topics != null && !topics.isEmpty()) {
            this.whitelist = Collections.unmodifiableSet(new LinkedHashSet<>(topics));
            log.info("Topic 过滤已启用，白名单: {}", whitelist);
        } else {
            this.whitelist = Collections.emptySet();
            log.info("Topic 过滤未启用（白名单为空），所有 Topic 将被同步");
        }
    }

    /**
     * 从逗号分隔的字符串创建过滤器
     */
    public static TopicFilter fromCommaSeparated(String topicsStr) {
        if (topicsStr == null || topicsStr.trim().isEmpty()) {
            return new TopicFilter(null);
        }
        Set<String> topics = new LinkedHashSet<>();
        for (String t : topicsStr.split(",")) {
            String trimmed = t.trim();
            if (!trimmed.isEmpty()) {
                topics.add(trimmed);
            }
        }
        return new TopicFilter(topics);
    }

    public void setMetricsCollector(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    /**
     * 检查 Topic 是否应该被同步
     *
     * @param topic 消息 Topic
     * @return true 表示应该同步，false 表示应跳过
     */
    public boolean accept(String topic) {
        if (whitelist.isEmpty()) {
            return true;  // 未配置白名单，全部通过
        }

        boolean accepted = whitelist.contains(topic);
        if (!accepted && metricsCollector != null) {
            metricsCollector.incrementFilteredMessageCount();
        }
        return accepted;
    }

    /**
     * 获取当前白名单
     */
    public Set<String> getWhitelist() {
        return whitelist;
    }

    /**
     * 是否启用了过滤
     */
    public boolean isEnabled() {
        return !whitelist.isEmpty();
    }
}
