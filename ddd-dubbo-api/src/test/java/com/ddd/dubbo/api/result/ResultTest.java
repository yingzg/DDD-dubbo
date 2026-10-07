package com.ddd.dubbo.api.result;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResultTest {
    @Test
    void successSetsCodeZeroAndData() {
        Result<String> r = Result.success("ok");
        assertTrue(r.isSuccess());
        assertEquals(0, r.getCode());
        assertEquals("ok", r.getData());
    }

    @Test
    void failSetsCodeAndMsg() {
        Result<Void> r = Result.fail(2001, "订单不存在");
        assertFalse(r.isSuccess());
        assertEquals(2001, r.getCode());
        assertEquals("订单不存在", r.getMsg());
    }

    @Test
    void withTraceIdAttachesTraceId() {
        Result<Void> r = Result.<Void>fail(5000, "系统异常").withTraceId("t-123");
        assertEquals("t-123", r.getTraceId());
    }
}
