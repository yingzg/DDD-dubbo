package com.ddd.dubbo.domain.order.entity;

import java.math.BigDecimal;

/** 订单聚合根 */
public class Order {

    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount = BigDecimal.ZERO;
    private String status;
    private Long createdAt;   // UTC 毫秒时间戳
    private Long updatedAt;   // UTC 毫秒时间戳

    public void applyDiscount(BigDecimal d) {
        if (d != null) {
            this.discount = this.discount.add(d);
        }
    }

    public void pay() {
        this.status = com.ddd.dubbo.domain.order.statemachine.OrderStatus.PAID.name();
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
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
    public Long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Long updatedAt) { this.updatedAt = updatedAt; }
}
