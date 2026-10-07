package com.ddd.dubbo.app.mq;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;

@Component
public class MqProducerTemplate {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public void send(String topic, Object payload) {
        rocketMQTemplate.convertAndSend(topic, payload);
    }
}
