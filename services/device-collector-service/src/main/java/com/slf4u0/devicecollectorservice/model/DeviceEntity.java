package com.slf4u0.devicecollectorservice.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "devices")
public class DeviceEntity {

    @Id
    @Column(name = "device_id", nullable = false, length = 255)
    private String deviceId;

    @Column(name = "device_type", nullable = false, length = 100)
    private String deviceType;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "meta", columnDefinition = "TEXT")
    private String meta;

}
