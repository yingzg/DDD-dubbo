package com.ddd.dubbo.infra.outbox.repository;

import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import com.ddd.dubbo.domain.common.time.TimeZoneUtils;
import com.ddd.dubbo.infra.outbox.dataobject.OutboxDO;
import com.ddd.dubbo.infra.outbox.mapper.OutboxMapper;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@Repository
public class OutboxRepositoryImpl implements OutboxRepository {

    @Resource
    private OutboxMapper outboxMapper;

    @Override
    public void save(OutboxEvent event) {
        OutboxDO outboxDO = new OutboxDO();
        outboxDO.setEventType(event.getEventType());
        outboxDO.setPayload(event.getPayload());
        outboxDO.setStatus(event.getStatus() == null ? OutboxEvent.STATUS_PENDING : event.getStatus());
        outboxDO.setRetryCount(event.getRetryCount() == null ? 0 : event.getRetryCount());
        outboxDO.setCreateTime(TimeZoneUtils.getCurrentUtcTimestamp());
        outboxMapper.insert(outboxDO);
    }

    @Override
    public List<OutboxEvent> selectPending(int limit) {
        List<OutboxDO> list = outboxMapper.selectPending(limit);
        List<OutboxEvent> result = new ArrayList<OutboxEvent>();
        for (OutboxDO outboxDO : list) {
            OutboxEvent e = new OutboxEvent();
            e.setId(outboxDO.getId());
            e.setEventType(outboxDO.getEventType());
            e.setPayload(outboxDO.getPayload());
            e.setStatus(outboxDO.getStatus());
            e.setRetryCount(outboxDO.getRetryCount());
            result.add(e);
        }
        return result;
    }

    @Override
    public void markSent(Long id) {
        outboxMapper.markSent(id);
    }

    @Override
    public void increaseRetry(Long id) {
        outboxMapper.increaseRetry(id);
    }
}
