package com.ddd.dubbo.domain.order.exe;

import com.ddd.dubbo.domain.common.exception.Assert;
import com.ddd.dubbo.domain.common.exception.OrderErrorCode;
import com.ddd.dubbo.domain.exe.QryExe;
import com.ddd.dubbo.domain.order.command.QueryOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class QueryOrderQryExe implements QryExe<QueryOrderCommand, Order> {

    @Resource
    private OrderRepository orderRepository;

    @Override
    public Order execute(QueryOrderCommand qry) {
        Order order = orderRepository.findById(qry.getOrderId());
        Assert.notNull(order, OrderErrorCode.ORDER_NOT_FOUND);
        return order;
    }
}
