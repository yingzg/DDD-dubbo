package com.ddd.dubbo.domain.outbox;

import java.io.Serializable;

/** Outbox 事件（领域模型）：业务 + 消息表同库同事务，最终一致 */
public class OutboxEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int STATUS_PENDING = 0;
    public static final int STATUS_SENT = 1;

    private Long id;
    private String eventType;      // 事件类型，如 ORDER_CREATED
    private String payload;        // 消息体 JSON
    private Integer status;        // 0=PENDING 1=SENT
    private Integer retryCount;    // 重试次数

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public Integer getStatus() { return status; }
    public void setStatus(Integer status) { this.status = status; }
    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }
}
