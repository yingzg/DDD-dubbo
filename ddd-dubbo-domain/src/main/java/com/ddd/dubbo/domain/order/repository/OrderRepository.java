package com.ddd.dubbo.domain.order.repository;

import com.ddd.dubbo.domain.order.entity.Order;

public interface OrderRepository {
    Order findById(Long id);
    void save(Order order);
}
