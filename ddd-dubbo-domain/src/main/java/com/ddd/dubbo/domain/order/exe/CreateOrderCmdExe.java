package com.ddd.dubbo.domain.order.exe;

import com.ddd.dubbo.domain.extension.BizScenario;
import com.ddd.dubbo.domain.extension.ExtensionExecutor;
import com.ddd.dubbo.domain.exe.CmdExe;
import com.ddd.dubbo.domain.order.command.CreateOrderCommand;
import com.ddd.dubbo.domain.order.entity.Order;
import com.ddd.dubbo.domain.order.extension.OrderDiscountExtPt;
import com.ddd.dubbo.domain.order.service.OrderService;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import javax.annotation.Resource;
import java.math.BigDecimal;

@Component
public class CreateOrderCmdExe implements CmdExe<CreateOrderCommand, Order> {

    @Resource
    private OrderService orderService;
    @Resource
    private ExtensionExecutor extensionExecutor;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Order execute(CreateOrderCommand cmd) {
        Order order = orderService.createOrder(cmd.getUserId(), cmd.getAmount());
        OrderDiscountExtPt ext = extensionExecutor.execute(
                OrderDiscountExtPt.class, BizScenario.of(cmd.getChannel()));
        BigDecimal discount = ext.calculateDiscount(order);
        order.applyDiscount(discount);
        orderService.save(order);
        return order;
    }
}
