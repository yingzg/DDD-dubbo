package com.ddd.dubbo.job;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.ddd.dubbo")
public class JobStartApplication {
    public static void main(String[] args) {
        SpringApplication.run(JobStartApplication.class, args);
    }
}
