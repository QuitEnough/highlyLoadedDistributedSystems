package com.slf4u0.eventscollectorservice.outbox;

import com.slf4u0.eventscollectorservice.producer.DeviceIdPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxProcessor {

    private final DeviceIdPublisher deviceIdPublisher;
    private final DeviceOutboxRepository outboxRepo;
    private final RedisLock redisLock;

    @Scheduled(fixedDelay = 10_000)
    public void processOutbox() {
        String lockKey = "outbox:lock";
        String instanceId = java.util.UUID.randomUUID().toString();
        if (!redisLock.tryLock(lockKey, instanceId, 5000)) {
            return; // другой инстанс работает
        }

        try {
            var records = outboxRepo.findNewRecords(100);
            for (var record : records) {
                // 🔑 Вторая дедупликация: защита от повторной публикации
                if (redisLock.saddIfAbsent("published:devices", record.getDeviceId(), 3600)) {
                    try {
                        deviceIdPublisher.sendDeviceId(record.getDeviceId());
                        outboxRepo.markAsSent(record.getDeviceId());
                    } catch (Exception e) {
                        outboxRepo.incrementAttempts(record.getDeviceId(), e.getMessage());
                    }
                }
            }
        } finally {
            redisLock.unlock(lockKey, instanceId);
        }

//        try {
//            var records = outboxRepo.findNewRecords(100);
//            for (var record : records) {
//                try {
//                    deviceIdPublisher.sendDeviceId(record.getDeviceId());
//                    outboxRepo.markAsSent(record.getDeviceId());
//                } catch (Exception e) {
//                    outboxRepo.incrementAttempts(record.getDeviceId(), e.getMessage());
//                }
//            }
//        } finally {
//            redisLock.unlock(lockKey, instanceId);
//        }

        //куда добавить??
        //// Перед отправкой:
        //if (redisLock.saddIfAbsent("published:devices", record.getDeviceId(), 3600)) {
        //    deviceIdPublisher.publishDeviceId(...);
        //}

//        // 1. Захватить lock
//        // 2. Выбрать записи из device_outbox WHERE status = 0
//        for (DeviceOutboxRecord record : records) {
//            try {
//                deviceIdPublisher.sendDeviceId(record.getDeviceId());
//                // 3. Пометить как SENT
//            } catch (Exception e) {
//                // 4. Увеличить attempts, сохранить ошибку
//            }
//        }
//        // 5. Освободить lock
    }

}
