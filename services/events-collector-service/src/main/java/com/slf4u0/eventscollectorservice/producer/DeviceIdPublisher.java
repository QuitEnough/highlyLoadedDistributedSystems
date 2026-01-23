package com.slf4u0.eventscollectorservice.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DeviceIdPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendDeviceId(String deviceId) {
       kafkaTemplate.send("devices", deviceId)
               .whenComplete((result, ex) -> {
                   if (ex == null) {
                       // Успешно отправлено
                   } else {
                       // Обработка ошибки
                   }
               });
    }
}
