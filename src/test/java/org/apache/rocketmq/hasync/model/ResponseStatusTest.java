package org.apache.rocketmq.hasync.model;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * ResponseStatus 枚举单元测试
 */
public class ResponseStatusTest {

    @Test
    public void testAllValues() {
        ResponseStatus[] values = ResponseStatus.values();
        assertEquals(4, values.length);
    }

    @Test
    public void testValueOf() {
        assertEquals(ResponseStatus.SUCCESS, ResponseStatus.valueOf("SUCCESS"));
        assertEquals(ResponseStatus.NO_NEW_MSG, ResponseStatus.valueOf("NO_NEW_MSG"));
        assertEquals(ResponseStatus.OFFSET_ILLEGAL, ResponseStatus.valueOf("OFFSET_ILLEGAL"));
        assertEquals(ResponseStatus.ERROR, ResponseStatus.valueOf("ERROR"));
    }
}
