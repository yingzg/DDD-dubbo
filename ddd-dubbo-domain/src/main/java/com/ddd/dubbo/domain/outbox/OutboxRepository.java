package com.ddd.dubbo.domain.outbox;

import java.util.List;

/** Outbox 仓储接口（domain 定义，infra 实现） */
public interface OutboxRepository {
    void save(OutboxEvent event);
    List<OutboxEvent> selectPending(int limit);
    void markSent(Long id);
    void increaseRetry(Long id);
}
