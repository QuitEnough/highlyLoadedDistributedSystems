package com.slf4u0.eventscollectorservice.listener;

import com.slf4u0.avro.DeviceEvent;
import com.slf4u0.eventscollectorservice.deduplication.DeviceDeduplicator;
import com.slf4u0.eventscollectorservice.outbox.OutboxWriter;
import com.slf4u0.eventscollectorservice.repository.DeviceEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventsCollector {

    private final DeviceEventRepository deviceEventRepository;
    private final DeviceDeduplicator deviceDeduplicator;
    private final OutboxWriter outboxWriter;

    @KafkaListener(topics = "events")
    public void listen(DeviceEvent event) {
        // 1. Сохранить в device_events
        deviceEventRepository.save(event); // ← нужно реализовать

        // 2. Дедупликация по deviceId
        boolean isNewDevice = deviceDeduplicator.isNewDevice(event.getDeviceId());

        // 3. Если новый — создать запись в outbox
        if (isNewDevice) {
            outboxWriter.createOutboxRecord(event.getDeviceId());
        }
    }

}
