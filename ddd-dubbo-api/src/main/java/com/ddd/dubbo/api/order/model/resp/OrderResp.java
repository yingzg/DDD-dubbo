package com.ddd.dubbo.api.order.model.resp;

import java.io.Serializable;
import java.math.BigDecimal;

public class OrderResp implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long orderId;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount;
    private String status;
    private Long createdAt;   // UTC 毫秒时间戳

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public Long getUserId() { return userId; }
    public void setUserId(Long userId) { this.userId = userId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public BigDecimal getDiscount() { return discount; }
    public void setDiscount(BigDecimal discount) { this.discount = discount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Long getCreatedAt() { return createdAt; }
    public void setCreatedAt(Long createdAt) { this.createdAt = createdAt; }
}
