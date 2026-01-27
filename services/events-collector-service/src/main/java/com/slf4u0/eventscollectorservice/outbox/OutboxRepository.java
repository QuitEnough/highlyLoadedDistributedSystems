package com.slf4u0.eventscollectorservice.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

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

    private final Connection clickhouseDataSource;

    public void insert(String deviceId) {
        String sql = """
            INSERT INTO device_outbox (device_id, created_at, status, sent_at, attempts, last_error)
            VALUES (?, ?, 0, toDateTime(0), 0, '')
            """;
        try (PreparedStatement stmt = clickhouseDataSource.prepareStatement(sql)) {
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
        try (PreparedStatement stmt = clickhouseDataSource.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                DeviceOutboxRecord r = new DeviceOutboxRecord();
                r.setDeviceId(rs.getString("device_id"));
                r.setCreatedAt((LocalDateTime) rs.getObject("created_at"));
                r.setStatus(rs.getByte("status"));
                r.setSentAt((LocalDateTime) rs.getObject("sent_at"));
                r.setAttempts(rs.getInt("attempts"));
                r.setLastError(rs.getString("last_error"));
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
        try (PreparedStatement stmt = clickhouseDataSource.prepareStatement(sql)) {
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
        try (PreparedStatement stmt = clickhouseDataSource.prepareStatement(sql)) {
            stmt.setString(1, error);
            stmt.setString(2, deviceId);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to increment attempts", e);
        }
    }

}
