package com.ddd.dubbo.infra.order.converter;

import com.ddd.dubbo.api.order.model.resp.OrderResp;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.statemachine.OrderStatus;
import com.ddd.dubbo.infra.order.dataobject.OrderDO;

public final class OrderConverter {

    private OrderConverter() {
    }

    public static Order toEntity(OrderDO orderDO) {
        if (orderDO == null) {
            return null;
        }
        Order order = new Order();
        order.setId(orderDO.getId());
        order.setUserId(orderDO.getUserId());
        order.setAmount(orderDO.getAmount());
        order.setDiscount(orderDO.getDiscount());
        order.setStatus(OrderStatus.valueOf(orderDO.getStatus()));
        order.setCreatedAt(orderDO.getCreateTime());
        order.setUpdatedAt(orderDO.getUpdateTime());
        return order;
    }

    public static OrderDO toDO(Order order) {
        OrderDO orderDO = new OrderDO();
        orderDO.setId(order.getId());
        orderDO.setUserId(order.getUserId());
        orderDO.setAmount(order.getAmount());
        orderDO.setDiscount(order.getDiscount());
        orderDO.setStatus(order.getStatus().name());
        orderDO.setCreateTime(order.getCreatedAt());
        orderDO.setUpdateTime(order.getUpdatedAt());
        return orderDO;
    }

    public static OrderResp toResp(Order order) {
        OrderResp resp = new OrderResp();
        resp.setOrderId(order.getId());
        resp.setUserId(order.getUserId());
        resp.setAmount(order.getAmount());
        resp.setDiscount(order.getDiscount());
        resp.setStatus(order.getStatus().name());
        resp.setCreatedAt(order.getCreatedAt());
        return resp;
    }
}
