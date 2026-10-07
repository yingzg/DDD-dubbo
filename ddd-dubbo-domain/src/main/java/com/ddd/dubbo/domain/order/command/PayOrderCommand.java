package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.domain.exe.Command;
import com.ddd.dubbo.domain.order.entity.Order;

import java.math.BigDecimal;

public class PayOrderCommand implements Command<Order> {
    private Long orderId;
    private BigDecimal balance;   // 支付余额（演示状态机 when 条件）

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public BigDecimal getBalance() { return balance; }
    public void setBalance(BigDecimal balance) { this.balance = balance; }
}
