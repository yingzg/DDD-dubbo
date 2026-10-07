package com.ddd.dubbo.app.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(RedissonProperties.class)
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedissonProperties props) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + props.getHost() + ":" + props.getPort());
        return Redisson.create(config);
    }
}
