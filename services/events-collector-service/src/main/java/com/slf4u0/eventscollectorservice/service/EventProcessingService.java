package com.slf4u0.eventscollectorservice.service;

import com.slf4u0.avro.DeviceEvent;
import com.slf4u0.eventscollectorservice.deduplication.DeviceDeduplicator;
import com.slf4u0.eventscollectorservice.outbox.OutboxRepository;
import com.slf4u0.eventscollectorservice.repository.DeviceEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventProcessingService {

    private final DeviceEventRepository deviceEventRepository;
    private final OutboxRepository outboxRepository;
    private final DeviceDeduplicator deviceDeduplicator;

    @Transactional
    public void processEvent(DeviceEvent event) {
        log.info("Processing event for deviceId: {}", event.getDeviceId());

        // 1. Дедупликация: проверяем, видели ли мы этот deviceId раньше.
        boolean isNewDevice = deviceDeduplicator.checkAndAddDevice(event.getDeviceId());

        // 2. Сохранение основного события в ClickHouse.
        // Здесь потребуется маппинг из Avro Event в Entity.
        // Предположим, что DeviceEvent уже является Entity или мы делаем маппинг.
        deviceEventRepository.save(event);
        log.info("Saved event to device_events table.");

        // 3. Если устройство новое, добавляем запись в исходящий Outbox.
        if (isNewDevice) {
            // Здесь нужен класс OutboxEntity с полями device_id, status=0, created_at
            // outboxRepository.save(new OutboxEntity(event.getDeviceId(), ...));
            log.info("New device detected, added to outbox for publishing.");
        }

    }

}
