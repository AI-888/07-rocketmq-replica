package org.apache.rocketmq.hasync.sink;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * FixedQueueSelector 单元测试
 */
public class FixedQueueSelectorTest {

    private FixedQueueSelector selector;

    @Before
    public void setUp() {
        selector = new FixedQueueSelector();
    }

    @Test
    public void testSelectSameQueueId() {
        // 目标队列数 >= 源 queueId → 直接映射
        assertEquals(0, selector.select(4, 0));
        assertEquals(1, selector.select(4, 1));
        assertEquals(3, selector.select(4, 3));
    }

    @Test
    public void testSelectWithModulo() {
        // 目标队列数 < 源 queueId → 取模
        assertEquals(1, selector.select(4, 5));  // 5 % 4 = 1
        assertEquals(2, selector.select(3, 8));  // 8 % 3 = 2
        assertEquals(0, selector.select(2, 10)); // 10 % 2 = 0
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectNegativeQueueId() {
        selector.select(4, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectZeroTotalQueues() {
        selector.select(0, 0);
    }

    @Test
    public void testQueueConfigConsistency() {
        assertTrue(selector.isQueueConfigConsistent(4, 4));
        assertFalse(selector.isQueueConfigConsistent(4, 8));
        assertFalse(selector.isQueueConfigConsistent(8, 4));
    }

    @Test
    public void testSelectEdgeCases() {
        assertEquals(0, selector.select(1, 0));   // 只有 1 个队列
        assertEquals(0, selector.select(1, 100)); // 取模 100 % 1 = 0
        assertEquals(99, selector.select(100, 99)); // 大队列直接映射
    }
}
