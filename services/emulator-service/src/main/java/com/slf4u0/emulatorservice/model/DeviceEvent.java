package com.slf4u0.emulatorservice.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@ToString
public class DeviceEvent {

    private String eventId;
    private String deviceId;
    private String eventType;
    private String payload;
    private Instant timestamp;

    public DeviceEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
    }

    public DeviceEvent(String deviceId, String eventType, String payload) {
        this();
        this.deviceId = deviceId;
        this.eventType = eventType;
        this.payload = payload;
    }
}
