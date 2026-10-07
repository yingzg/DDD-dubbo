package com.ddd.dubbo.infra.order.dataobject;

import java.math.BigDecimal;

public class OrderDO {
    private Long id;
    private Long userId;
    private BigDecimal amount;
    private BigDecimal discount;
    private String status;
    private Long createTime;   // UTC 毫秒时间戳
    private Long updateTime;   // UTC 毫秒时间戳

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
    public Long getCreateTime() { return createTime; }
    public void setCreateTime(Long createTime) { this.createTime = createTime; }
    public Long getUpdateTime() { return updateTime; }
    public void setUpdateTime(Long updateTime) { this.updateTime = updateTime; }
}
