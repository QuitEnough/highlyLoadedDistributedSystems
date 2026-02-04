package com.slf4u0.eventscollectorservice.service;

import com.slf4u0.eventscollectorservice.metrics.AppMetrics;
import com.slf4u0.eventscollectorservice.producer.DeviceIdPublisher;
import com.slf4u0.eventscollectorservice.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxProcessor {

    private final OutboxRepository outboxRepository;
    private final DeviceIdPublisher deviceIdPublisher;
    private final RedisLock redisLock;
    private final AppMetrics metrics;

    @Scheduled(fixedDelay = 10_000)
    public void processOutbox() {
        String lockKey = "outbox:lock";
        String instanceId = java.util.UUID.randomUUID().toString();
        if (!redisLock.tryLock(lockKey, instanceId, 5000)) {
            return;
        }

        try {
            var records = outboxRepository.findNewRecords(100);
            for (var record : records) {
                if (!redisLock.isDevicePublished(record.deviceId(), 3600)) {
                    try {
                        deviceIdPublisher.sendDeviceId(record.deviceId());
                        outboxRepository.markAsSent(record.deviceId());
                    } catch (Exception e) {
                        outboxRepository.incrementAttempts(record.deviceId(), e.getMessage());
                        metrics.incrementOutboxFail();
                    }
                } else {
                    outboxRepository.markAsSent(record.deviceId());
                    metrics.incrementEventsDuplicates();
                }
            }
        } finally {
            redisLock.unlock(lockKey, instanceId);
        }
    }

}
