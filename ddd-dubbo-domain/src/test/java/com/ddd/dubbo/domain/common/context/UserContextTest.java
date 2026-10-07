package com.ddd.dubbo.domain.common.context;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UserContextTest {

    @Test
    void setAndGetThenClear() {
        UserContext.set(UserContext.builder().userId(100L).locale("zh_CN").build());
        assertNotNull(UserContext.get());
        assertEquals(Long.valueOf(100L), UserContext.get().getUserId());
        assertEquals("zh_CN", UserContext.get().getLocale());
        UserContext.clear();
        assertNull(UserContext.get());
    }

    @Test
    void traceContextReturnsNullWhenNoOtel() {
        // 无 OTel Agent 时，Span.current() 为 invalid，返回 null
        assertNull(TraceContext.getTraceId());
    }
}
