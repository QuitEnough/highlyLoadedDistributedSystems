package com.slf4u0.devicecollectorservice.repository;

import com.slf4u0.devicecollectorservice.model.DeviceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface DeviceRepository extends JpaRepository<DeviceEntity, String> {

    Optional<DeviceEntity> findByDeviceId(String deviceId);

    @Query(
            value = "SELECT * FROM devices WHERE device_id = :deviceId",
            nativeQuery = true
    )
    Optional<DeviceEntity> findByDeviceIdWithSharding(@Param("deviceId") String deviceId);

}
