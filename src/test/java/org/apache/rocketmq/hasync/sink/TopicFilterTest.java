package org.apache.rocketmq.hasync.sink;

import org.apache.rocketmq.hasync.metrics.MetricsCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * TopicFilter 单元测试
 */
public class TopicFilterTest {

    private MetricsCollector metricsCollector;

    @Before
    public void setUp() {
        metricsCollector = new MetricsCollector();
    }

    @Test
    public void testEmptyWhitelistAcceptsAll() {
        TopicFilter filter = new TopicFilter(null);
        assertTrue(filter.accept("TopicA"));
        assertTrue(filter.accept("TopicB"));
        assertTrue(filter.accept("AnyTopic"));
    }

    @Test
    public void testEmptySetAcceptsAll() {
        TopicFilter filter = new TopicFilter(new HashSet<>());
        assertTrue(filter.accept("TopicA"));
        assertFalse(filter.isEnabled());
    }

    @Test
    public void testWhitelistAccepts() {
        Set<String> whitelist = new HashSet<>(Arrays.asList("TopicA", "TopicB"));
        TopicFilter filter = new TopicFilter(whitelist);

        assertTrue(filter.accept("TopicA"));
        assertTrue(filter.accept("TopicB"));
        assertFalse(filter.accept("TopicC"));
    }

    @Test
    public void testWhitelistRejectsWithMetrics() {
        Set<String> whitelist = new HashSet<>(Arrays.asList("TopicA"));
        TopicFilter filter = new TopicFilter(whitelist);
        filter.setMetricsCollector(metricsCollector);

        filter.accept("TopicB"); // 被拒绝
        filter.accept("TopicC"); // 被拒绝
        filter.accept("TopicA"); // 通过

        assertEquals(2, metricsCollector.getFilteredMessageCount());
    }

    @Test
    public void testFromCommaSeparated() {
        TopicFilter filter = TopicFilter.fromCommaSeparated("TopicA, TopicB, TopicC");
        assertTrue(filter.accept("TopicA"));
        assertTrue(filter.accept("TopicB"));
        assertTrue(filter.accept("TopicC"));
        assertFalse(filter.accept("TopicD"));
    }

    @Test
    public void testFromCommaSeparatedNull() {
        TopicFilter filter = TopicFilter.fromCommaSeparated(null);
        assertTrue(filter.accept("AnyTopic"));
        assertFalse(filter.isEnabled());
    }

    @Test
    public void testFromCommaSeparatedEmpty() {
        TopicFilter filter = TopicFilter.fromCommaSeparated("");
        assertTrue(filter.accept("AnyTopic"));
        assertFalse(filter.isEnabled());
    }

    @Test
    public void testFromCommaSeparatedWithSpaces() {
        TopicFilter filter = TopicFilter.fromCommaSeparated("  TopicA ,  TopicB  ");
        assertTrue(filter.accept("TopicA"));
        assertTrue(filter.accept("TopicB"));
    }

    @Test
    public void testGetWhitelist() {
        Set<String> whitelist = new HashSet<>(Arrays.asList("TopicA", "TopicB"));
        TopicFilter filter = new TopicFilter(whitelist);
        assertEquals(2, filter.getWhitelist().size());
        assertTrue(filter.getWhitelist().contains("TopicA"));
    }

    @Test
    public void testIsEnabled() {
        TopicFilter enabled = new TopicFilter(new HashSet<>(Arrays.asList("TopicA")));
        assertTrue(enabled.isEnabled());

        TopicFilter disabled = new TopicFilter(null);
        assertFalse(disabled.isEnabled());
    }
}
