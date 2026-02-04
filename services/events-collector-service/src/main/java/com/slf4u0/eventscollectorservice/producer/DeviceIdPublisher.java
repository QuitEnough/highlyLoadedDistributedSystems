package com.slf4u0.eventscollectorservice.producer;

import com.slf4u0.eventscollectorservice.metrics.AppMetrics;
import com.slf4u0.eventscollectorservice.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeviceIdPublisher {

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;
    private static final String DEVICES_TOPIC = "devices";
    private final OutboxRepository outboxRepository;
    private final AppMetrics metrics;

    // подумать как можно внедрить через ObjectMapper
    private static final Schema DEVICE_ID_SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "  \"type\": \"record\",\n" +
                    "  \"name\": \"DeviceIdEvent\",\n" +
                    "  \"namespace\": \"com.slf4u0.avro\",\n" +
                    "  \"fields\": [\n" +
                    "    { \"name\": \"deviceId\", \"type\": \"string\" }\n" +
                    "  ]\n" +
                    "}"
    );

    public void sendDeviceId(String deviceId) {
        GenericRecord record = new GenericData.Record(DEVICE_ID_SCHEMA);
        record.put("deviceId", deviceId);

        log.info("Publishing deviceId {} to topic {} as Avro", deviceId, DEVICES_TOPIC);
        kafkaTemplate.send(DEVICES_TOPIC, deviceId, record)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent deviceId: {}", deviceId);
                        outboxRepository.markAsSent(deviceId);
                        metrics.incrementOutboxPublishSuccess();
                    } else {
                        log.error("Failed to publish deviceId: {}. Error: {}", deviceId, ex.getMessage());
                        outboxRepository.incrementAttempts(deviceId, ex.getMessage());
                    }
                });
    }
}
