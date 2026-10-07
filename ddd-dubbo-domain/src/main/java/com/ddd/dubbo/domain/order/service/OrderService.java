package com.ddd.dubbo.domain.order.service;

import com.ddd.dubbo.domain.common.time.TimeZoneUtils;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;

import java.math.BigDecimal;

public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public Order createOrder(Long userId, BigDecimal amount) {
        Order order = new Order();
        order.setUserId(userId);
        order.setAmount(amount);
        order.setStatus(OrderStatus.INIT.name());
        order.setCreatedAt(TimeZoneUtils.getCurrentUtcTimestamp());
        order.setUpdatedAt(order.getCreatedAt());
        return order;
    }

    public void save(Order order) {
        orderRepository.save(order);
    }
}
