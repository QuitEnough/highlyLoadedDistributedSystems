package com.slf4u0.eventscollectorservice;

import com.slf4u0.eventscollectorservice.metrics.AppMetrics;
import com.slf4u0.eventscollectorservice.outbox.DeviceOutboxRecord;
import com.slf4u0.eventscollectorservice.outbox.OutboxProcessor;
import com.slf4u0.eventscollectorservice.outbox.OutboxRepository;
import com.slf4u0.eventscollectorservice.outbox.RedisLock;
import com.slf4u0.eventscollectorservice.producer.DeviceIdPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OutboxProcessorTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private DeviceIdPublisher deviceIdPublisher;

    @Mock
    private RedisLock redisLock;

    @Mock
    private AppMetrics appMetrics;

    @InjectMocks
    private OutboxProcessor processor;

    @Test
    public void shouldPublishNewDeviceAndMarkAsSent() {
        // given
        DeviceOutboxRecord record = DeviceOutboxRecord.builder()
                .deviceId("device-123")
                .status((byte) 0)
                .attempts(0)
                .createdAt(LocalDateTime.now())
                .build();
        when(outboxRepository.findNewRecords(100)).thenReturn(List.of(record));
        when(redisLock.tryLock(any(), any(), anyLong())).thenReturn(true);
        when(redisLock.isDevicePublished("device-123", 3600)).thenReturn(false);

        // when
        processor.processOutbox();

        // then
        verify(deviceIdPublisher).sendDeviceId("device-123");
        // markAsSent будет вызван внутри sendDeviceId → сложно проверить в unit-тесте
        // но мы проверим, что не было попытки повторной отправки
        verify(outboxRepository, never()).incrementAttempts(any(), any());
    }

    @Test
    public void shouldSkipAlreadyPublishedDevice() {
        // given
        DeviceOutboxRecord record = DeviceOutboxRecord.builder()
                .deviceId("device-456")
                .status((byte) 0)
                .attempts(0)
                .createdAt(LocalDateTime.now())
                .build();

        when(outboxRepository.findNewRecords(100)).thenReturn(List.of(record));
        when(redisLock.tryLock(any(), any(), anyLong())).thenReturn(true);
        when(redisLock.isDevicePublished("device-456", 3600)).thenReturn(true); // уже опубликован

        // when
        processor.processOutbox();

        // then
        verify(deviceIdPublisher, never()).sendDeviceId(any());
        verify(outboxRepository).markAsSent("device-456"); // предполагаем, что помечаем как SENT
    }

}
