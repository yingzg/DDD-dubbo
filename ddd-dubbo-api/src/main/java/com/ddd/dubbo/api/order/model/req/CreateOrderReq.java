package com.ddd.dubbo.api.order.model.req;

import java.io.Serializable;
import java.math.BigDecimal;

public class CreateOrderReq implements Serializable {
    private static final long serialVersionUID = 1L;
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
