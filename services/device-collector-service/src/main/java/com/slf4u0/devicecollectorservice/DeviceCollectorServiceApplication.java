package com.slf4u0.devicecollectorservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication(exclude = FlywayAutoConfiguration.class)
public class DeviceCollectorServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeviceCollectorServiceApplication.class, args);
    }

}
