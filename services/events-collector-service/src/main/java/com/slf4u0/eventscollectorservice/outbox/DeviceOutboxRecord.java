package com.slf4u0.eventscollectorservice.outbox;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DeviceOutboxRecord {

    private String deviceId;
    private LocalDateTime createdAt;
    private byte status;
    private LocalDateTime sentAt;
    private int attempts;
    private String lastError;

}
