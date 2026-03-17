package org.apache.rocketmq.hasync.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * PullRequest 模型单元测试
 */
public class PullRequestTest {

    @Test
    public void testDefaultConstructor() {
        PullRequest req = new PullRequest();
        assertNull(req.getTopicFilter());
        assertEquals(0L, req.getFromOffset());
        assertEquals(0, req.getBatchSize());
        assertNull(req.getSinkId());
    }

    @Test
    public void testParameterizedConstructor() {
        PullRequest req = new PullRequest(1000L, 50, "sink-01");
        assertEquals(1000L, req.getFromOffset());
        assertEquals(50, req.getBatchSize());
        assertEquals("sink-01", req.getSinkId());
        assertNull(req.getTopicFilter());
    }

    @Test
    public void testSettersAndGetters() {
        PullRequest req = new PullRequest();

        req.setFromOffset(5000L);
        assertEquals(5000L, req.getFromOffset());

        req.setBatchSize(200);
        assertEquals(200, req.getBatchSize());

        req.setSinkId("my-sink");
        assertEquals("my-sink", req.getSinkId());

        Set<String> topicFilter = new HashSet<>(Arrays.asList("topic-a", "topic-b"));
        req.setTopicFilter(topicFilter);
        assertEquals(topicFilter, req.getTopicFilter());
        assertTrue(req.getTopicFilter().contains("topic-a"));
        assertTrue(req.getTopicFilter().contains("topic-b"));
    }

    @Test
    public void testToString() {
        PullRequest req = new PullRequest(1000L, 100, "sink-01");
        String str = req.toString();
        assertTrue(str.contains("fromOffset=1000"));
        assertTrue(str.contains("batchSize=100"));
        assertTrue(str.contains("sink-01"));
    }
}
