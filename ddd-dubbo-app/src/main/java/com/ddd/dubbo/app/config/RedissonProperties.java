package com.ddd.dubbo.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ddd.dubbo.redisson")
public class RedissonProperties {
    private String host = "127.0.0.1";
    private int port = 6379;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
}
