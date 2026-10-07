package com.ddd.dubbo.app.config.threadpool;

import com.alibaba.ttl.threadpool.TtlExecutors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;

/** 场景化线程池（TTL 包装透传上下文） */
@Configuration
public class ThreadPoolConfig {

    @Bean("commonTaskPool")
    public ExecutorService commonTaskPool() {
        return ttl(executor(10, 50, 1000, "common-"));
    }

    @Bean("exportTaskPool")
    public ExecutorService exportTaskPool() {
        return ttl(executor(4, 16, 500, "export-"));
    }

    private ThreadPoolTaskExecutor executor(int core, int max, int queue, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(core);
        executor.setMaxPoolSize(max);
        executor.setQueueCapacity(queue);
        executor.setThreadNamePrefix(prefix);
        executor.initialize();
        return executor;
    }

    private ExecutorService ttl(ThreadPoolTaskExecutor executor) {
        return TtlExecutors.getTtlExecutorService(executor.getThreadPoolExecutor());
    }
}
