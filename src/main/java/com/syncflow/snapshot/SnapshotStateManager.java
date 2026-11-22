
package com.syncflow.snapshot;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SnapshotStateManager {

    private final Connection connection;

    public SnapshotStateManager(Connection connection) {
        this.connection = Objects.requireNonNull(
                connection, "Connection cannot be null");
    }

    /**
     * Create bootstrap metadata tables if they do not exist.
     * A new installation starts with status NEW.
     */
    public void initialize() throws SQLException {

        try (Statement statement = connection.createStatement()) {

            statement.execute("""
                CREATE TABLE IF NOT EXISTS bootstrap_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    status VARCHAR(30) NOT NULL
                        CHECK (status IN (
                            'NEW', 'COPYING', 'CATCHING_UP',
                            'STREAMING', 'FAILED'
                        )),
                    snapshot_lsn TEXT,
                    started_at TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
                """);

            statement.execute("""
                CREATE TABLE IF NOT EXISTS table_state (
                    table_name VARCHAR(100) PRIMARY KEY,
                    status VARCHAR(30) NOT NULL
                        CHECK (status IN (
                            'PENDING', 'COPYING', 'COPIED', 'FAILED'
                        )),
                    rows_copied BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);

            statement.execute("ALTER TABLE bootstrap_state ADD COLUMN IF NOT EXISTS slot_name VARCHAR(100)");
            statement.execute("ALTER TABLE bootstrap_state ADD COLUMN IF NOT EXISTS publication_name VARCHAR(100)");
            statement.execute("ALTER TABLE table_state ADD COLUMN IF NOT EXISTS target_table_name VARCHAR(100)");
        }

        try (PreparedStatement statement = connection.prepareStatement("""
                INSERT INTO bootstrap_state (id, status)
                VALUES (1, 'NEW')
                ON CONFLICT (id) DO NOTHING
                """)) {

            statement.executeUpdate();
        }
    }

    /**
     * Begin a new bootstrap and register the tables owned by SyncFlow.
     * This changes metadata only; it does not modify business tables.
     */
    public void beginBootstrap(
            String snapshotLsn,
            List<String> ownedTables) throws SQLException {

        Objects.requireNonNull(ownedTables, "Owned tables cannot be null");
        Map<String, String> mapping = new LinkedHashMap<>();
        for (String table : ownedTables) {
            mapping.put(table, table);
        }
        beginBootstrap(snapshotLsn, mapping);
    }

    /**
     * Begin a new bootstrap and register the source-to-target table mapping.
     */
    public void beginBootstrap(
            String snapshotLsn,
            Map<String, String> tableMapping) throws SQLException {

        Objects.requireNonNull(snapshotLsn, "Snapshot LSN cannot be null");
        Objects.requireNonNull(tableMapping, "Table mapping cannot be null");

        if (tableMapping.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one table mapping is required");
        }

        boolean previousAutoCommit = connection.getAutoCommit();

        try {
            connection.setAutoCommit(false);

            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE bootstrap_state
                    SET status = 'COPYING',
                        snapshot_lsn = ?,
                        started_at = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP,
                        error_message = NULL
                    WHERE id = 1
                    """)) {

                update.setString(1, snapshotLsn);

                if (update.executeUpdate() != 1) {
                    throw new SQLException(
                            "Bootstrap state has not been initialized");
                }
            }

            try (Statement delete = connection.createStatement()) {
                delete.executeUpdate("DELETE FROM table_state");
            }

            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO table_state (table_name, target_table_name, status)
                    VALUES (?, ?, 'PENDING')
                    """)) {

                for (Map.Entry<String, String> entry : tableMapping.entrySet()) {
                    insert.setString(1, entry.getKey());
                    insert.setString(2, entry.getValue());
                    insert.addBatch();
                }

                insert.executeBatch();
            }

            connection.commit();

        } catch (SQLException | RuntimeException e) {

            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                e.addSuppressed(rollbackError);
            }

            throw e;

        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    public void markTableCopying(String tableName)
            throws SQLException {

        updateTableStatus(tableName, "COPYING", null);
    }

    public void markTableCopied(String tableName, long rowsCopied)
            throws SQLException {

        if (rowsCopied < 0) {
            throw new IllegalArgumentException(
                    "Rows copied cannot be negative");
        }

        updateTableStatus(tableName, "COPIED", rowsCopied);
    }

    public void markTableFailed(String tableName)
            throws SQLException {

        updateTableStatus(tableName, "FAILED", null);
    }

    private void updateTableStatus(
            String tableName,
            String status,
            Long rowsCopied) throws SQLException {

        String sql = rowsCopied == null
                ? """
                  UPDATE table_state
                  SET status = ?, updated_at = CURRENT_TIMESTAMP
                  WHERE table_name = ?
                  """
                : """
                  UPDATE table_state
                  SET status = ?, rows_copied = ?,
                      updated_at = CURRENT_TIMESTAMP
                  WHERE table_name = ?
                  """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, status);

            if (rowsCopied == null) {
                statement.setString(2, tableName);
            } else {
                statement.setLong(2, rowsCopied);
                statement.setString(3, tableName);
            }

            if (statement.executeUpdate() != 1) {
                throw new SQLException(
                        "Unknown bootstrap table: " + tableName);
            }
        }
    }

    public void markCatchingUp() throws SQLException {
        updateBootstrapStatus("CATCHING_UP", null);
    }

    public void markStreaming() throws SQLException {
        updateBootstrapStatus("STREAMING", null);
    }

    public void markFailed(String errorMessage) throws SQLException {
        updateBootstrapStatus("FAILED", errorMessage);
    }

    private void updateBootstrapStatus(
            String status,
            String errorMessage) throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement("""
                UPDATE bootstrap_state
                SET status = ?,
                    updated_at = CURRENT_TIMESTAMP,
                    error_message = ?
                WHERE id = 1
                """)) {

            statement.setString(1, status);
            statement.setString(2, errorMessage);

            if (statement.executeUpdate() != 1) {
                throw new SQLException(
                        "Bootstrap state has not been initialized");
            }
        }
    }

    public String getBootstrapStatus() throws SQLException {

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT status
                     FROM bootstrap_state
                     WHERE id = 1
                     """)) {

            if (!rs.next()) {
                throw new SQLException(
                        "Bootstrap state has not been initialized");
            }

            return rs.getString("status");
        }
    }

    public String getSnapshotLsn() throws SQLException {

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT snapshot_lsn
                     FROM bootstrap_state
                     WHERE id = 1
                     """)) {

            if (!rs.next()) {
                throw new SQLException(
                        "Bootstrap state has not been initialized");
            }

            return rs.getString("snapshot_lsn");
        }
    }

    public List<String> getOwnedTables() throws SQLException {

        List<String> tables = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT table_name
                     FROM table_state
                     ORDER BY table_name
                     """)) {

            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        }

        return tables;
    }

    public Map<String, String> getTableMapping() throws SQLException {
        Map<String, String> map = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT table_name, target_table_name
                     FROM table_state
                     ORDER BY table_name
                     """)) {
            while (rs.next()) {
                String src = rs.getString("table_name");
                String tgt = rs.getString("target_table_name");
                map.put(src, tgt != null && !tgt.isBlank() ? tgt : src);
            }
        }
        return map;
    }

    public Map<String, String> getTableStatuses() throws SQLException {
        Map<String, String> map = new LinkedHashMap<>();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT table_name, status
                     FROM table_state
                     ORDER BY table_name
                     """)) {
            while (rs.next()) {
                map.put(rs.getString("table_name"), rs.getString("status"));
            }
        }
        return map;
    }

    public void recordSlotAndPublication(
            String slotName,
            String publicationName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("""
                UPDATE bootstrap_state
                SET slot_name = ?,
                    publication_name = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """)) {
            ps.setString(1, slotName);
            ps.setString(2, publicationName);
            ps.executeUpdate();
        }
    }

    public String getSlotName() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT slot_name FROM bootstrap_state WHERE id = 1")) {
            if (rs.next()) {
                return rs.getString("slot_name");
            }
            return null;
        }
    }

    public String getPublicationName() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT publication_name FROM bootstrap_state WHERE id = 1")) {
            if (rs.next()) {
                return rs.getString("publication_name");
            }
            return null;
        }
    }

    public void resetToNew() throws SQLException {
        boolean previousAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("""
                        UPDATE bootstrap_state
                        SET status = 'NEW',
                            snapshot_lsn = NULL,
                            started_at = NULL,
                            error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = 1
                        """);
                stmt.executeUpdate("""
                        UPDATE table_state
                        SET status = 'PENDING',
                            rows_copied = 0,
                            updated_at = CURRENT_TIMESTAMP
                        """);
            }
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }
}
