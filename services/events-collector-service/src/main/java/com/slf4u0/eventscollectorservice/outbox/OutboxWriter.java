package com.slf4u0.eventscollectorservice.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class OutboxWriter {

    private final DataSource clickhouseDataSource;

    public void createOutboxRecord(String deviceId) {
        String sql = """
            INSERT INTO device_outbox (
                device_id, created_at, status, sent_at, attempts, last_error
            ) VALUES (?, ?, 0, toDateTime(0), 0, '')
            """;

        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, deviceId);
            stmt.setObject(2, LocalDateTime.now());
            stmt.execute();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to outbox", e);
        }
    }

}
