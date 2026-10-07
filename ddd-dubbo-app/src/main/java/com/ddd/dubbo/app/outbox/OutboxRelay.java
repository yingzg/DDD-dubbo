package com.ddd.dubbo.app.outbox;

import com.ddd.dubbo.app.mq.MqProducerTemplate;
import com.ddd.dubbo.domain.outbox.OutboxEvent;
import com.ddd.dubbo.domain.outbox.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/** Outbox 投递器：定时扫描 PENDING 消息投递 MQ，成功标记 SENT */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    @Resource
    private OutboxRepository outboxRepository;
    @Resource
    private MqProducerTemplate mqProducerTemplate;

    @Scheduled(fixedDelay = 5000)
    public void relay() {
        List<OutboxEvent> pending = outboxRepository.selectPending(BATCH_SIZE);
        for (OutboxEvent e : pending) {
            try {
                mqProducerTemplate.send(e.getEventType(), e.getPayload());
                outboxRepository.markSent(e.getId());
            } catch (Exception ex) {
                log.error("[outbox] 投递失败 id={} type={}", e.getId(), e.getEventType(), ex);
                outboxRepository.increaseRetry(e.getId());
            }
        }
    }
}
