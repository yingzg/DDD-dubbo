package com.ddd.dubbo.domain.order.command;

import com.ddd.dubbo.domain.exe.Command;
import com.ddd.dubbo.domain.order.entity.Order;
import java.math.BigDecimal;

public class CreateOrderCommand implements Command<Order> {
    private Long userId;
    private String channel;
    private BigDecimal amount;

    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
}
