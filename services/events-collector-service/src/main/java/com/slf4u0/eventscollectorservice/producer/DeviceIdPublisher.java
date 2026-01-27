package com.slf4u0.eventscollectorservice.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeviceIdPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String DEVICES_TOPIC = "devices";

    public void sendDeviceId(String deviceId) {
        log.info("Publishing deviceId {} to topic {}", deviceId, DEVICES_TOPIC);
        kafkaTemplate.send(DEVICES_TOPIC, deviceId)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent deviceId: {}", deviceId);
                    } else {
                        // В логах увидим причину, Outbox зафиксирует статус и попробует снова в след. цикле
                        log.error("Failed to publish deviceId: {}. Error: {}", deviceId, ex.getMessage());
                    }
                });
    }
}
