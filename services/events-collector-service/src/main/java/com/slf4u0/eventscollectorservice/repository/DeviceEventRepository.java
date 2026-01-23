package com.slf4u0.eventscollectorservice.repository;

import com.slf4u0.avro.DeviceEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

@Repository
@RequiredArgsConstructor
public class DeviceEventRepository {

    private final DataSource clickhouseDataSource;

    public void save(DeviceEvent event) {
        String sql = """
            INSERT INTO device_events (
                device_id, event_id, event_date, timestamp_ms, type, payload
            ) VALUES (?, ?, ?, ?, ?, ?)
            """;

        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

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
        } catch (Exception e) {
            throw new RuntimeException("Failed to save event to ClickHouse", e);
        }
    }

}
