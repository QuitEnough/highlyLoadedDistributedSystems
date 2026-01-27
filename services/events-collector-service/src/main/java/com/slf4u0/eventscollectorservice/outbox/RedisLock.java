package com.slf4u0.eventscollectorservice.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RedisLock {

    private final StringRedisTemplate redisTemplate;

    public boolean tryLock(String key, String value, long ttlMs) {
        Boolean result = redisTemplate.opsForValue()
                .setIfAbsent(key, value, java.time.Duration.ofMillis(ttlMs));
        return Boolean.TRUE.equals(result);
    }

    public void unlock(String lockKey, String expectedValue) {
        String currentValue = redisTemplate.opsForValue().get(lockKey);
        if (expectedValue.equals(currentValue)) {
            redisTemplate.delete(lockKey);
        }
    }

    public boolean isDevicePublished(String deviceId, int ttlSeconds) {
        Boolean exists = redisTemplate.opsForSet().isMember("published:devices", deviceId);
        if (Boolean.TRUE.equals(exists)) {
            return true; // Already published
        }

        // Atomically add and set TTL
        Long added = redisTemplate.opsForSet().add("published:devices", deviceId);
        if (added != null && added > 0) {
            redisTemplate.expire("published:devices", java.time.Duration.ofSeconds(ttlSeconds));
            return false; // New device, not published yet
        }
        return true; // Race condition: someone else added simultaneously
    }

}
