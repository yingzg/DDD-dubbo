package com.ddd.dubbo.domain.order.exe;

import com.ddd.dubbo.domain.common.event.DomainEventPublisher;
import com.ddd.dubbo.domain.common.exception.Assert;
import com.ddd.dubbo.domain.common.exception.OrderErrorCode;
import com.ddd.dubbo.domain.exe.CmdExe;
import com.ddd.dubbo.domain.order.command.PayOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.event.OrderPaidEvent;
import com.ddd.dubbo.domain.order.repository.OrderRepository;
import com.ddd.dubbo.domain.order.statemachine.OrderContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

@Component
public class PayOrderCmdExe implements CmdExe<PayOrderCommand, Order> {

    @Resource
    private OrderRepository orderRepository;
    @Resource
    private DomainEventPublisher domainEventPublisher;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Order execute(PayOrderCommand cmd) {
        Order order = orderRepository.findById(cmd.getOrderId());
        Assert.notNull(order, OrderErrorCode.ORDER_NOT_FOUND);

        OrderContext ctx = new OrderContext();
        ctx.setBalance(cmd.getBalance());
        ctx.setAmount(order.getAmount());
        order.pay(ctx);   // 状态机流转（余额校验）

        orderRepository.save(order);
        domainEventPublisher.publishAsync(new OrderPaidEvent(order.getId(), order.getAmount()));
        return order;
    }
}
