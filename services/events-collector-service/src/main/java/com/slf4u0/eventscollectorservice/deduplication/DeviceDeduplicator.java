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

    private final StatefulRedisConnection<String, String> lettuceConnection;
    private static final String SEEN_DEVICES_KEY = "devices:seen";
    private static final Duration TTL = Duration.ofHours(1);

    /**
     * Проверяет, был ли deviceId уже добавлен.
     * @param deviceId Идентификатор устройства.
     * @return true, если deviceId был добавлен впервые, иначе false.
     */
    public boolean checkAndAddDeviceId(String deviceId) {
        RedisCommands<String, String> syncCommands = lettuceConnection.sync();

        // SADD возвращает 1, если элемент новый, и 0, если уже существует.
        Long addedCount = syncCommands.sadd(SEEN_DEVICES_KEY, deviceId);

        if (addedCount == 1L) {
            // Если добавлен впервые, устанавливаем TTL для ключа.
            syncCommands.expire(SEEN_DEVICES_KEY, TTL);
            return true; // Это новое устройство
        }
        return false; // Это уже известное устройство
    }

}
