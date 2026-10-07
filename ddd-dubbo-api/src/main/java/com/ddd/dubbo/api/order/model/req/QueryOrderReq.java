package com.ddd.dubbo.api.order.model.req;

import java.io.Serializable;

public class QueryOrderReq implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long orderId;

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
}
