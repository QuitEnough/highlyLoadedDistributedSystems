package com.slf4u0.eventscollectorservice;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class DockerConnectionTest {

    @Test
    void shouldStartRedisContainer() {
        try (GenericContainer<?> redis = new GenericContainer<>("redis:7.2")) {
            redis.start();
            assertThat(redis.isRunning()).isTrue();
        }
    }

}
