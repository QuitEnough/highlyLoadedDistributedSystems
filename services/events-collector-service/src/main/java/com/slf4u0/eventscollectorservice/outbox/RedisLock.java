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

    // Для идемпотентности публикации
    public boolean saddIfAbsent(String key, String value, int ttlSeconds) {
        Long result = redisTemplate.opsForSet().add(key, value);
        if (result != null && result > 0) {
            redisTemplate.expire(key, java.time.Duration.ofSeconds(ttlSeconds));
            return true;
        }
        return false;
    }

}
