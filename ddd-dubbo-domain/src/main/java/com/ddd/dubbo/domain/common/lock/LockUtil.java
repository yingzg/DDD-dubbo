package com.ddd.dubbo.domain.common.lock;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** 分布式锁工具（Redisson） */
@Component
public class LockUtil {

    private static final int LOCK_TIMEOUT = 5003;

    private final RedissonClient redissonClient;

    public LockUtil(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    /** 单锁：带看门狗自动续期，获取超时抛异常 */
    public <T> T lock(String key, long waitSeconds, Supplier<T> action) {
        RLock lock = redissonClient.getLock(key);
        try {
            if (!lock.tryLock(waitSeconds, TimeUnit.SECONDS)) {
                throw new BizException(LOCK_TIMEOUT, "获取锁超时");
            }
            return action.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BizException(LOCK_TIMEOUT, "获取锁被中断");
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** 多重锁：按固定顺序加锁，降低死锁风险 */
    public RLock multiLock(String... keys) {
        RLock[] locks = new RLock[keys.length];
        for (int i = 0; i < keys.length; i++) {
            locks[i] = redissonClient.getLock(keys[i]);
        }
        return redissonClient.getMultiLock(locks);
    }

    /** 锁 key 生成：统一前缀 + 业务维度 */
    public String lockKey(String biz, String... dimensions) {
        return "lock:" + biz + ":" + String.join(":", dimensions);
    }
}
