package com.ddd.dubbo.domain.common.lock;

import com.ddd.dubbo.domain.common.exception.BizException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LockUtilTest {

    @Test
    void lockKeyFormatsWithPrefix() {
        RedissonClient client = mock(RedissonClient.class);
        LockUtil util = new LockUtil(client);
        assertEquals("lock:order:100", util.lockKey("order", "100"));
    }

    @Test
    void lockAcquiresAndRunsActionThenUnlocks() throws InterruptedException {
        RedissonClient client = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(client.getLock("lock:order:100")).thenReturn(lock);
        when(lock.tryLock(5L, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        LockUtil util = new LockUtil(client);
        String result = util.lock("lock:order:100", 5, () -> "done");

        assertEquals("done", result);
        verify(lock).unlock();
    }

    @Test
    void lockThrowsWhenTimeout() throws InterruptedException {
        RedissonClient client = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        when(client.getLock("k")).thenReturn(lock);
        when(lock.tryLock(5L, TimeUnit.SECONDS)).thenReturn(false);

        LockUtil util = new LockUtil(client);
        assertThrows(BizException.class, () -> util.lock("k", 5, () -> "never"));
    }
}
