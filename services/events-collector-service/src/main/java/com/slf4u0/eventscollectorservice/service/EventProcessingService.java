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

    public void processEvent(DeviceEvent event) {
        log.info("Processing event for deviceId: {}", event.getDeviceId());

        deviceEventRepository.save(event);
        log.info("Saved event to device_events table.");

        boolean isNewDevice = deviceDeduplicator.isNewDevice(event.getDeviceId());

        if (isNewDevice) {
            outboxRepository.insert(event.getDeviceId());
            log.info("New device detected, added to outbox for publishing.");
        }

    }

}
