package com.slf4u0.emulatorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private KafkaTemplate<String, String> kafkaTemplate;
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
