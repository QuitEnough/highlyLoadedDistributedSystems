package com.slf4u0.devicecollectorservice.consumer;

import com.slf4u0.avro.Device;
import com.slf4u0.devicecollectorservice.exception.NotRetryableException;
import com.slf4u0.devicecollectorservice.exception.RetryableException;
import com.slf4u0.devicecollectorservice.model.DeviceEntity;
import com.slf4u0.devicecollectorservice.repository.DeviceRepository;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hibernate.type.SerializationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceKafkaListener {

    private final DeviceRepository deviceRepository;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 5000, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            dltTopicSuffix = "-dlt",
            retryTopicSuffix = "-retry",
            include = {RetryableException.class},
            exclude = {NotRetryableException.class}
    )
    @KafkaListener(
            topics = "devices",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void listenDeviceEvents(
            ConsumerRecord<String, Device> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        try {
           Device device = record.value();
           log.info("Received device: {}, from topic: {}, offset: {}",
                   device.getDeviceId(), topic, offset);

            Optional<DeviceEntity> existingDevice = deviceRepository.findByDeviceId(device.getDeviceId());

            DeviceEntity deviceEntity;
            if (existingDevice.isPresent()) {
                deviceEntity = existingDevice.get();
                deviceEntity.setDeviceType(device.getDeviceType().toString());
                deviceEntity.setMeta(device.getMeta().toString());
                log.info("Updating existing device: {}", device.getDeviceId());
            } else {
                deviceEntity = new DeviceEntity();
                deviceEntity.setDeviceId(device.getDeviceId().toString());
                deviceEntity.setDeviceType(device.getDeviceType().toString());
                deviceEntity.setMeta(device.getMeta().toString());
                deviceEntity.setCreatedAt(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(device.getCreatedAt()),
                        ZoneId.systemDefault()));
                log.info("Creating new device: {}", device.getDeviceId());
            }

            deviceRepository.save(deviceEntity);
            ack.acknowledge();
            log.info("Successfully processed device: {}", device.getDeviceId());

        } catch (Exception e) {
            log.error("Error processing device message: {}", e.getMessage());
            if (e instanceof SerializationException || e instanceof RestClientException) {
                throw new NotRetryableException("Serialization error", e);
            }
            throw new RetryableException("Database error, will retry", e);
        }
    }

    @DltHandler
    public void handleDlt(
            Device device,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("Message moved to DLT. Device: {}, Original topic: {}, Error: {}",
                device.getDeviceId(), topic, exceptionMessage);
    }

}
