package com.slf4u0.eventscollectorservice.repository;

import com.slf4u0.eventscollectorservice.model.DeviceOutboxRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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
@Slf4j
public class OutboxRepository {

    private final DataSource clickhouseDataSource;

    @Transactional
    public void insert(String deviceId) {
        String sql = """
            INSERT INTO iot_analytics.device_outbox (
                device_id, created_at, status, sent_at, attempts, last_error
            ) VALUES (?, ?, 0, toDateTime(0), 0, '')
            """;

        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, deviceId);
            stmt.setObject(2, LocalDateTime.now());
            stmt.execute();

            log.debug("Inserted device {} into outbox", deviceId);

        } catch (SQLException e) {
            log.error("Failed to insert into outbox", e);
            throw new RuntimeException("Failed to insert into outbox", e);
        }
    }

    public List<DeviceOutboxRecord> findNewRecords(int limit) {
        String sql = """
            SELECT device_id, created_at, status, sent_at, attempts, last_error
            FROM iot_analytics.device_outbox
            WHERE status = 0
            ORDER BY created_at ASC
            LIMIT ?
            """;

        List<DeviceOutboxRecord> records = new ArrayList<>();
        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, limit);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                DeviceOutboxRecord record = DeviceOutboxRecord.builder()
                        .deviceId(rs.getString("device_id"))
                        .createdAt((LocalDateTime) rs.getObject("created_at"))
                        .status(rs.getByte("status"))
                        .sentAt((LocalDateTime) rs.getObject("sent_at"))
                        .attempts(rs.getInt("attempts"))
                        .lastError(rs.getString("last_error"))
                        .build();

                records.add(record);
            }

        } catch (SQLException e) {
            log.error("Failed to select from outbox", e);
            throw new RuntimeException("Failed to select from outbox", e);
        }

        return records;
    }
    @Transactional
    public void markAsSent(String deviceId) {
        String sql = """
            ALTER TABLE iot_analytics.device_outbox
            UPDATE status = 1, sent_at = ?
            WHERE device_id = ? AND status = 0
            """;

        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, LocalDateTime.now());
            stmt.setString(2, deviceId);
            int updated = stmt.executeUpdate();

            if (updated > 0) {
                log.debug("Marked device {} as sent in outbox", deviceId);
            }

        } catch (SQLException e) {
            log.error("Failed to update outbox status", e);
            throw new RuntimeException("Failed to update outbox status", e);
        }
    }
    @Transactional
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
        String sql = "SELECT count() FROM iot_analytics.device_outbox WHERE status = 0";

        try (Connection conn = clickhouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;

        } catch (SQLException e) {
            log.error("Failed to count pending outbox records", e);
            throw new RuntimeException("Failed to count pending outbox records", e);
        }
    }

}
