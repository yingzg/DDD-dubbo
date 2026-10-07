package com.ddd.dubbo.domain.common.exception;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class AssertTest {

    @Test
    void notNullThrowsWhenNull() {
        BizException e = assertThrows(BizException.class, () -> Assert.notNull(null, OrderErrorCode.ORDER_NOT_FOUND));
        assertEquals(2001, e.getCode());
    }

    @Test
    void notNullPassesWhenNotNull() {
        Assert.notNull("x", OrderErrorCode.ORDER_NOT_FOUND);
    }

    @Test
    void isTrueThrowsWhenFalse() {
        assertThrows(BizException.class, () -> Assert.isTrue(false, OrderErrorCode.STOCK_NOT_ENOUGH));
    }

    @Test
    void notEmptyThrowsWhenEmpty() {
        assertThrows(BizException.class, () -> Assert.notEmpty(Collections.emptyList(), OrderErrorCode.ORDER_STATUS_ERROR));
    }

    @Test
    void bizExceptionCarriesCodeAndMsg() {
        BizException e = new BizException(OrderErrorCode.ORDER_NOT_FOUND);
        assertEquals(2001, e.getCode());
        assertEquals("订单不存在", e.getMessage());
    }
}
