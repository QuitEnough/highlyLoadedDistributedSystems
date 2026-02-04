package com.slf4u0.emulatorservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import com.slf4u0.avro.DeviceEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

@Service
@Slf4j
public class MessageGeneratorService {

    @Autowired
    private KafkaTemplate<String, DeviceEvent> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    private DeviceEvent createAvroEvent(String deviceId, String eventType, String payload) {
        DeviceEvent event = new DeviceEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setDeviceId(deviceId);
        event.setTimestamp(Instant.now().toEpochMilli());
        event.setType(eventType);
        event.setPayload(payload);
        return event;
    }

    public void sendMultipleMessages(int count) {
        log.info("Sending {} AVRO messages...", count);
        IntStream.range(0, count).forEach(i -> {
            try {
                Thread.sleep(10);
                String deviceId = "device-" + random.nextInt(1000);
                String eventType = getRandomEventType();
                String payload = generateRandomPayload(); // см. ниже
                DeviceEvent event = createAvroEvent(deviceId, eventType, payload);
                kafkaTemplate.send("events", deviceId, event);
                if (i % 1000 == 0) {
                    log.info("Sent {} AVRO messages...", i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        log.info("Finished sending {} AVRO messages.", count);
    }

    // Вспомогательный метод вместо generateRandomEvent()
    private String generateRandomPayload() {
        Map<String, Object> data = new HashMap<>();
        data.put("temperature", 20 + random.nextGaussian() * 10);
        data.put("humidity", 30 + random.nextDouble() * 40);
        data.put("batteryLevel", 100 - random.nextInt(40));
        data.put("location", getRandomLocation());
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            log.error("Failed to generate payload", e);
            return "{}";
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
        sendScriptData(controllerId, eventType, payload); // переиспользуем
    }

    // Method to simulate script sending data
    public void sendScriptData(String deviceId, String eventType, String payload) {
        DeviceEvent event = createAvroEvent(deviceId, eventType, payload);
        kafkaTemplate.send("events", deviceId, event);
        log.info("Sent script event to Kafka: {}", event);
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
