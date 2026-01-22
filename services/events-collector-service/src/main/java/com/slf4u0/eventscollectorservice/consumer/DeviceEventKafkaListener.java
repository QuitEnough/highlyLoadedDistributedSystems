package com.slf4u0.eventscollectorservice.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Slf4j
@Component
//@RequiredArgsConstructor
public class DeviceEventKafkaListener {

//    private final EventProcessingService eventProcessingService;

//    @KafkaListener(topics = "events", groupId = "events-collector")
//    public void listen(DeviceEvent event, Acknowledgment ack) {
//        try {
//            eventProcessingService.process(event);
//            ack.acknowledge(); // ← подтверждение offset
//        } catch (Exception e) {
//            log.error("Failed to process event {}: {}", event.getEventId(), e.getMessage(), e);
//            // Не подтверждаем offset → Kafka повторит
//    }
}
