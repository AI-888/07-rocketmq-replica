package org.apache.rocketmq.hasync.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * PullResponse 模型单元测试
 */
public class PullResponseTest {

    @Test
    public void testDefaultConstructor() {
        PullResponse resp = new PullResponse();
        assertNull(resp.getRecords());
        assertEquals(0L, resp.getMaxOffset());
        assertNull(resp.getStatus());
    }

    @Test
    public void testConstructorWithRecords() {
        List<SyncRecord> records = new ArrayList<>();
        SyncRecord r1 = new SyncRecord();
        r1.setPhysicOffset(100L);
        records.add(r1);

        PullResponse resp = new PullResponse(records, 200L);
        assertEquals(1, resp.getRecords().size());
        assertEquals(200L, resp.getMaxOffset());
        assertEquals(ResponseStatus.SUCCESS, resp.getStatus());
    }

    @Test
    public void testConstructorWithStatus() {
        PullResponse resp = new PullResponse(ResponseStatus.NO_NEW_MSG);
        assertEquals(ResponseStatus.NO_NEW_MSG, resp.getStatus());
        assertNull(resp.getRecords());
        assertEquals(0L, resp.getMaxOffset());
    }

    @Test
    public void testSettersAndGetters() {
        PullResponse resp = new PullResponse();

        List<SyncRecord> records = new ArrayList<>();
        records.add(new SyncRecord());
        resp.setRecords(records);
        assertEquals(1, resp.getRecords().size());

        resp.setMaxOffset(9999L);
        assertEquals(9999L, resp.getMaxOffset());

        resp.setStatus(ResponseStatus.ERROR);
        assertEquals(ResponseStatus.ERROR, resp.getStatus());
    }

    @Test
    public void testToStringWithRecords() {
        List<SyncRecord> records = new ArrayList<>();
        records.add(new SyncRecord());
        records.add(new SyncRecord());
        PullResponse resp = new PullResponse(records, 500L);

        String str = resp.toString();
        assertTrue(str.contains("recordCount=2"));
        assertTrue(str.contains("maxOffset=500"));
        assertTrue(str.contains("SUCCESS"));
    }

    @Test
    public void testToStringWithNullRecords() {
        PullResponse resp = new PullResponse(ResponseStatus.NO_NEW_MSG);
        String str = resp.toString();
        assertTrue(str.contains("recordCount=0"));
    }
}
