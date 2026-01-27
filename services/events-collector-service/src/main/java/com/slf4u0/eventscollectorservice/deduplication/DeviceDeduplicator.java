package com.slf4u0.eventscollectorservice.deduplication;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class DeviceDeduplicator {

    private final StringRedisTemplate redisTemplate;
    private static final String SEEN_DEVICES_KEY = "devices:seen";
    private static final Duration TTL = Duration.ofHours(1);

    /**
     * Проверяет, был ли deviceId уже обработан.
     * Использует Read-Through: сначала читаем, потом пишем.
     *
     * @return true, если deviceId новый
     */
    public boolean isNewDevice(String deviceId) {
        Boolean exists = redisTemplate.opsForSet().isMember(SEEN_DEVICES_KEY, deviceId);
        if (Boolean.TRUE.equals(exists)) {
            return false; // уже видели
        }

        // Атомарно добавляем и устанавливаем TTL
        Long added = redisTemplate.opsForSet().add(SEEN_DEVICES_KEY, deviceId);
        if (added != null && added > 0) {
            redisTemplate.expire(SEEN_DEVICES_KEY, TTL);
            return true;
        }
        return false; // гонка: кто-то добавил одновременно
    }

}
