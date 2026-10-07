package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.domain.exe.Query;
import com.ddd.dubbo.domain.order.entity.Order;

public class QueryOrderCommand implements Query<Order> {
    private Long orderId;

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
}
