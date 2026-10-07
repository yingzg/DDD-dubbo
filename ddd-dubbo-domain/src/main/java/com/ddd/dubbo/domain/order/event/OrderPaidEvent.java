package com.ddd.dubbo.domain.order.event;

import com.ddd.dubbo.domain.common.event.DomainEvent;

import java.math.BigDecimal;

public class OrderPaidEvent extends DomainEvent {
    private static final long serialVersionUID = 1L;

    private final Long orderId;
    private final BigDecimal amount;

    public OrderPaidEvent(Long orderId, BigDecimal amount) {
        this.orderId = orderId;
        this.amount = amount;
    }

    @Override
    public String eventType() {
        return "OrderPaid";
    }

    @Override
    public String payload() {
        return "{\"orderId\":" + orderId + ",\"amount\":" + amount + "}";
    }

    public Long getOrderId() { return orderId; }
    public BigDecimal getAmount() { return amount; }
}
