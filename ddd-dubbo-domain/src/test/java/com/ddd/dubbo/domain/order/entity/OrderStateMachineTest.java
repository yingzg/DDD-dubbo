package com.ddd.dubbo.domain.order.entity;

import com.ddd.dubbo.domain.common.exception.BizException;
import com.ddd.dubbo.domain.order.statemachine.OrderContext;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class OrderStateMachineTest {

    @Test
    void payMovesInitToPaidWhenBalanceEnough() {
        Order order = new Order();
        order.setStatus(OrderStatus.INIT);
        order.setAmount(new BigDecimal("100"));
        OrderContext ctx = new OrderContext();
        ctx.setBalance(new BigDecimal("100"));
        ctx.setAmount(new BigDecimal("100"));
        order.pay(ctx);
        assertEquals(OrderStatus.PAID, order.getStatus());
    }

    @Test
    void payThrowsWhenBalanceNotEnough() {
        Order order = new Order();
        order.setStatus(OrderStatus.INIT);
        order.setAmount(new BigDecimal("100"));
        OrderContext ctx = new OrderContext();
        ctx.setBalance(new BigDecimal("50"));
        ctx.setAmount(new BigDecimal("100"));
        assertThrows(BizException.class, () -> order.pay(ctx));
    }

    @Test
    void payThrowsOnIllegalTransition() {
        Order order = new Order();
        order.setStatus(OrderStatus.PAID);
        OrderContext ctx = new OrderContext();
        ctx.setBalance(new BigDecimal("100"));
        ctx.setAmount(new BigDecimal("100"));
        assertThrows(BizException.class, () -> order.pay(ctx));
    }
}
