package com.slf4u0.eventscollectorservice;

import com.slf4u0.eventscollectorservice.config.ClickHouseConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@EnableConfigurationProperties(ClickHouseConfig.class)
@SpringBootApplication
public class EventsCollectorServiceApplication {

    public static void main(String[] args) {
//        SpringApplication.run(EventsCollectorServiceApplication.class, args);
        SpringApplication app = new SpringApplication(EventsCollectorServiceApplication.class);
        app.run(args);
        System.out.println("✅ Application started. Check Kafka consumer logs.");
    }

}
