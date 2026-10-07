package com.ddd.dubbo.infra.order.repository;

import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.infra.order.converter.OrderConverter;
import com.ddd.dubbo.infra.order.dataobject.OrderDO;
import com.ddd.dubbo.infra.order.mapper.OrderMapper;
import org.springframework.stereotype.Repository;
import javax.annotation.Resource;

@Repository
public class OrderRepositoryImpl implements OrderRepository {

    @Resource
    private OrderMapper orderMapper;

    @Override
    public Order findById(Long id) {
        OrderDO orderDO = orderMapper.findById(id);
        return OrderConverter.toEntity(orderDO);
    }

    @Override
    public void save(Order order) {
        orderMapper.insert(OrderConverter.toDO(order));
    }
}
