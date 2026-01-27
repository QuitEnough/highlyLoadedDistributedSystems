package com.slf4u0.eventscollectorservice.listener;

import com.slf4u0.avro.DeviceEvent;
import com.slf4u0.eventscollectorservice.service.EventProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventsCollector {

    private final EventProcessingService eventProcessingService;

    @KafkaListener(topics = "events")
    public void listen(DeviceEvent event) {
        eventProcessingService.processEvent(event);
    }

}
