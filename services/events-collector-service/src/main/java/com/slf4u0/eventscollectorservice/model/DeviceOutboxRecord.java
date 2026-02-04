package com.slf4u0.eventscollectorservice.model;

import lombok.Builder;

import java.time.LocalDateTime;

@Builder
public record DeviceOutboxRecord
        (
                String deviceId,
                LocalDateTime createdAt,
                byte status,
                LocalDateTime sentAt,
                int attempts,
                String lastError
        ) {
}

