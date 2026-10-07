package com.ddd.dubbo.app.config;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DubboConfig {

    @Bean
    public ApplicationConfig applicationConfig() {
        ApplicationConfig config = new ApplicationConfig();
        config.setName("ddd-dubbo");
        return config;
    }

    @Bean
    public RegistryConfig registryConfig() {
        RegistryConfig config = new RegistryConfig();
        config.setAddress("nacos://${nacos.addrs:127.0.0.1:8848}");
        return config;
    }

    @Bean
    public ProtocolConfig protocolConfig() {
        ProtocolConfig config = new ProtocolConfig();
        config.setName("dubbo");
        config.setPort(-1);
        return config;
    }
}
