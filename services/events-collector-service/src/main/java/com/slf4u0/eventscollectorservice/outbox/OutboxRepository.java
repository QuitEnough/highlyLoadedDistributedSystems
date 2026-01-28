package com.slf4u0.eventscollectorservice.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {

    private final DataSource clickhouseDataSource;

    public void insert(String deviceId) {
        String sql = """
            INSERT INTO device_outbox (device_id, created_at, status, sent_at, attempts, last_error)
            VALUES (?, ?, 0, toDateTime(0), 0, '')
            """;
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, deviceId);
            stmt.setObject(2, LocalDateTime.now());
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert into outbox", e);
        }
    }

    public List<DeviceOutboxRecord> findNewRecords(int limit) {
        String sql = """
            SELECT device_id, created_at, status, sent_at, attempts, last_error
            FROM device_outbox
            WHERE status = 0
            ORDER BY created_at
            LIMIT ?
            """;
        List<DeviceOutboxRecord> records = new ArrayList<>();
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                DeviceOutboxRecord r = DeviceOutboxRecord.builder()
                        .deviceId(rs.getString("device_id"))
                        .createdAt((LocalDateTime) rs.getObject("created_at"))
                        .status(rs.getByte("status"))
                        .sentAt((LocalDateTime) rs.getObject("sent_at"))
                        .attempts(rs.getInt("attempts"))
                        .lastError(rs.getString("last_error"))
                        .build();
                records.add(r);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to select from outbox", e);
        }
        return records;
    }

    public void markAsSent(String deviceId) {
        String sql = """
            ALTER TABLE device_outbox
            UPDATE status = 1, sent_at = ?
            WHERE device_id = ?
            """;
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, LocalDateTime.now());
            stmt.setString(2, deviceId);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update outbox status", e);
        }
    }

    public void incrementAttempts(String deviceId, String error) {
        String sql = """
            ALTER TABLE device_outbox
            UPDATE attempts = attempts + 1, last_error = ?
            WHERE device_id = ?
            """;
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, error);
            stmt.setString(2, deviceId);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to increment attempts", e);
        }
    }

    public long countPending() {
        String sql = "SELECT count() FROM device_outbox WHERE status = 0";
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count pending outbox records", e);
        }
    }

}
