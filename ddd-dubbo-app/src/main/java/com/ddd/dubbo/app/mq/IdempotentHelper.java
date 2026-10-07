package com.ddd.dubbo.app.mq;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Component
public class IdempotentHelper {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public boolean tryConsume(String messageId) {
        Boolean ok = stringRedisTemplate.opsForValue()
                .setIfAbsent("idempotent:" + messageId, "1", 24, TimeUnit.HOURS);
        return Boolean.TRUE.equals(ok);
    }

    public void complete(String messageId) {
        stringRedisTemplate.opsForValue().set("idempotent:" + messageId + ":done", "1", 24, TimeUnit.HOURS);
    }
}
