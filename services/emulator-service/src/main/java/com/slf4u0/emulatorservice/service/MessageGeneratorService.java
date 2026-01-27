package com.slf4u0.emulatorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slf4u0.emulatorservice.model.DeviceEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Service
@Slf4j
public class MessageGeneratorService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    // Scheduled to run every 5 minutes
    @Scheduled(fixedRate = 300000) // 300000 ms = 5 minutes
    public void sendScheduledMessages() {
        log.info("Sending scheduled batch of messages...");
        sendMessage("iot-events", generateRandomEvent());
    }

    public void sendMultipleMessages(int count) {
        log.info("Sending {} messages...", count);
        for (int i = 0; i < count; i++) {
            try {
                Thread.sleep(10); // Small delay to avoid overwhelming
                sendMessage("iot-events", generateRandomEvent());
                if (i % 1000 == 0) {
                    log.info("Sent {} messages...", i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Finished sending {} messages.", count);
    }

    public void sendMessage(String topic, String message) {
        try {
            kafkaTemplate.send(topic, message);
        } catch (Exception e) {
            log.error("Error sending message to Kafka", e);
        }
    }

    // Method to send DeviceEvent objects
    public void sendDeviceEvent(String topic, DeviceEvent event) {
        try {
            kafkaTemplate.send(topic, event.getDeviceId(), event);
        } catch (Exception e) {
            log.error("Error sending DeviceEvent to Kafka", e);
        }
    }

    // Method to simulate controller sending data
    public void sendControllerData(String controllerId, String eventType, String payload) {
        DeviceEvent event = new DeviceEvent(controllerId, eventType, payload);
        kafkaTemplate.send("events", controllerId, event);
        log.info("Sent controller event to Kafka: {}", event);
    }

    // Method to simulate script sending data
    public void sendScriptData(String deviceId, String eventType, String payload) {
        DeviceEvent event = new DeviceEvent(deviceId, eventType, payload);
        kafkaTemplate.send("events", deviceId, event);
        log.info("Sent script event to Kafka: {}", event);
    }

    private String generateRandomEvent() {
        Map<String, Object> event = new HashMap<>();

        // Generate random IoT event data
        event.put("id", UUID.randomUUID().toString());
        event.put("timestamp", Instant.now().toEpochMilli());
        event.put("deviceId", "device-" + random.nextInt(1000));
        event.put("eventType", getRandomEventType());
        event.put("temperature", 20 + random.nextGaussian() * 10); // Normal distribution around 20°C
        event.put("humidity", 30 + random.nextDouble() * 40); // Between 30-70%
        event.put("batteryLevel", 100 - random.nextInt(40)); // Between 60-100%
        event.put("location", getRandomLocation());

        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("Error serializing event", e);
            return "{}";
        }
    }

    private String getRandomEventType() {
        String[] types = {"sensor_data", "status_update", "alert", "heartbeat", "configuration_change"};
        return types[random.nextInt(types.length)];
    }

    private Map<String, Double> getRandomLocation() {
        Map<String, Double> location = new HashMap<>();
        location.put("lat", 40.0 + random.nextDouble() * 0.1); // Around NYC area
        location.put("lon", -74.0 + random.nextDouble() * 0.1);
        return location;
    }

}
