package org.apache.rocketmq.hasync.source;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TransactionMessageFilter 单元测试（需求 21 §21.8）
 */
public class TransactionMessageFilterTest {

    private TransactionMessageFilter filter;

    @Before
    public void setUp() {
        filter = new TransactionMessageFilter();
    }

    @Test
    public void testShouldSkip_HalfTopic() {
        assertTrue("Half 消息应跳过",
                filter.shouldSkip("RMQ_SYS_TRANS_HALF_TOPIC"));
        assertEquals(1, filter.getSkipCount());
    }

    @Test
    public void testShouldSkip_OpTopic() {
        assertTrue("Op 消息应跳过",
                filter.shouldSkip("RMQ_SYS_TRANS_OP_HALF_TOPIC"));
        assertEquals(1, filter.getSkipCount());
    }

    @Test
    public void testShouldNotSkip_NormalTopic() {
        assertFalse("普通 Topic 不应跳过",
                filter.shouldSkip("OrderTopic"));
        assertEquals(0, filter.getSkipCount());
    }

    @Test
    public void testShouldNotSkip_CommittedTransactionMessage() {
        // 已提交的事务消息写入真实 Topic，不应被跳过
        assertFalse("已提交的事务消息（真实 Topic）不应跳过",
                filter.shouldSkip("PaymentTopic"));
    }

    @Test
    public void testShouldNotSkip_NullTopic() {
        assertFalse("null Topic 不应跳过", filter.shouldSkip(null));
    }

    @Test
    public void testSkipCountAccumulation() {
        filter.shouldSkip("RMQ_SYS_TRANS_HALF_TOPIC");
        filter.shouldSkip("RMQ_SYS_TRANS_OP_HALF_TOPIC");
        filter.shouldSkip("NormalTopic");
        filter.shouldSkip("RMQ_SYS_TRANS_HALF_TOPIC");
        assertEquals("跳过计数应为 3", 3, filter.getSkipCount());
    }

    @Test
    public void testConstants() {
        assertEquals("RMQ_SYS_TRANS_HALF_TOPIC", TransactionMessageFilter.TRANS_HALF_TOPIC);
        assertEquals("RMQ_SYS_TRANS_OP_HALF_TOPIC", TransactionMessageFilter.TRANS_OP_TOPIC);
    }
}
