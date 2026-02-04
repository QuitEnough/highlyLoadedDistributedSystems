package com.slf4u0.eventscollectorservice.repository;

import com.slf4u0.avro.DeviceEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

@Repository
@RequiredArgsConstructor
@Slf4j
public class DeviceEventRepository {

    private final DataSource clickhouseDataSource;
    private final static String SQL = """
            INSERT INTO iot_analytics.device_events (device_id, event_id, event_date, timestamp_ms, type, payload) 
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    // валидировать данные которые подаем на вход
    @Transactional
    public void save(DeviceEvent event) {
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SQL)) {

            long timestampMs = event.getTimestamp();
            LocalDate eventDate = Instant.ofEpochMilli(timestampMs)
                    .atZone(ZoneId.systemDefault())
                    .toLocalDate();

            stmt.setString(1, event.getDeviceId());
            stmt.setString(2, event.getEventId());
            stmt.setObject(3, eventDate);
            stmt.setLong(4, timestampMs);
            stmt.setString(5, event.getType());
            stmt.setString(6, event.getPayload());
            stmt.execute();

            log.debug("Saved event {} for device {}", event.getEventId(), event.getDeviceId());

        } catch (SQLException e) {
            log.error("Failed to save event to ClickHouse", e);
            throw new RuntimeException("Failed to save event to ClickHouse", e);
        }
    }

}
