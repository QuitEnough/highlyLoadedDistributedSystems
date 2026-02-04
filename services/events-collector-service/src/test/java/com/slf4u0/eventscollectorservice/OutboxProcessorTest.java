package com.slf4u0.eventscollectorservice;

import com.slf4u0.eventscollectorservice.metrics.AppMetrics;
import com.slf4u0.eventscollectorservice.model.DeviceOutboxRecord;
import com.slf4u0.eventscollectorservice.producer.DeviceIdPublisher;
import com.slf4u0.eventscollectorservice.repository.OutboxRepository;
import com.slf4u0.eventscollectorservice.service.OutboxProcessor;
import com.slf4u0.eventscollectorservice.service.RedisLock;
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

        processor.processOutbox();

        verify(deviceIdPublisher).sendDeviceId("device-123");
        verify(outboxRepository, never()).incrementAttempts(any(), any());
    }

    @Test
    public void shouldSkipAlreadyPublishedDevice() {
        DeviceOutboxRecord record = DeviceOutboxRecord.builder()
                .deviceId("device-456")
                .status((byte) 0)
                .attempts(0)
                .createdAt(LocalDateTime.now())
                .build();

        when(outboxRepository.findNewRecords(100)).thenReturn(List.of(record));
        when(redisLock.tryLock(any(), any(), anyLong())).thenReturn(true);
        when(redisLock.isDevicePublished("device-456", 3600)).thenReturn(true);

        processor.processOutbox();

        verify(deviceIdPublisher, never()).sendDeviceId(any());
        verify(outboxRepository).markAsSent("device-456");
    }

}
