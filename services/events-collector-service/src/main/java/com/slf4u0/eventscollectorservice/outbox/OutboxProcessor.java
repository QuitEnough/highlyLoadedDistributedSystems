package com.slf4u0.eventscollectorservice.outbox;

import com.slf4u0.eventscollectorservice.producer.DeviceIdPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxProcessor {

    private final OutboxRepository outboxRepository;
    private final DeviceIdPublisher deviceIdPublisher;
    private final RedisLock redisLock;

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
                if (!redisLock.isDevicePublished(record.getDeviceId(), 3600)) {
                    try {
                        deviceIdPublisher.sendDeviceId(record.getDeviceId());
                        outboxRepository.markAsSent(record.getDeviceId());
                    } catch (Exception e) {
                        outboxRepository.incrementAttempts(record.getDeviceId(), e.getMessage());
                    }
                } else {
                    // Device already published, mark as sent to avoid reprocessing
                    outboxRepository.markAsSent(record.getDeviceId());
                }
            }
        } finally {
            redisLock.unlock(lockKey, instanceId);
        }
    }

}
