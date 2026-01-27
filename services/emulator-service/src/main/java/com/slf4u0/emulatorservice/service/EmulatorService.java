package com.slf4u0.emulatorservice.service;

import com.slf4u0.emulatorservice.model.DeviceEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class EmulatorService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    private final Random random = new Random();

    // Scheduled task to push data to Kafka every 5 minutes
    @Scheduled(fixedRate = 300000) // 300000 ms = 5 minutes
    public void sendPeriodicDataToKafka() {
        DeviceEvent event = new DeviceEvent(
                "controller-" + (random.nextInt(100) + 1),
                "PERIODIC_TELEMETRY",
                "{\"temperature\": " + (20 + random.nextDouble() * 30) + ", \"humidity\": " + (30 + random.nextDouble() * 40) + ", \"status\": \"active\"}"
        );

        kafkaTemplate.send("events", event.getDeviceId(), event);
        System.out.println("Sent periodic telemetry event to Kafka: " + event);
    }

    // Method to simulate controller sending data
    public void sendControllerData(String controllerId, String eventType, String payload) {
        DeviceEvent event = new DeviceEvent(controllerId, eventType, payload);
        kafkaTemplate.send("events", controllerId, event);
        System.out.println("Sent controller event to Kafka: " + event);
    }

    // Method to simulate script sending data
    public void sendScriptData(String deviceId, String eventType, String payload) {
        DeviceEvent event = new DeviceEvent(deviceId, eventType, payload);
        kafkaTemplate.send("events", deviceId, event);
        System.out.println("Sent script event to Kafka: " + event);
    }

}
