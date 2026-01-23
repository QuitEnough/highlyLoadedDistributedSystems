package com.slf4u0.eventscollectorservice.deduplication;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisDeduplicator {

    private final StringRedisTemplate redisTemplate;
    private static final String KEY_PREFIX = "devices:seen";
    private static final Duration TTL = Duration.ofHours(1); // можно вынести в конфиг

    public boolean isNewDevice(String deviceId) {
        Long result = redisTemplate.opsForSet().add(KEY_PREFIX, deviceId);
        if (result != null && result > 0) {
            redisTemplate.expire(KEY_PREFIX, TTL);
            return true;
        }
        return false;
    }

}
