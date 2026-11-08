
package com.syncflow.reliability;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CheckpointManager {

    private final Connection connection;

    public CheckpointManager(Connection connection) throws SQLException {
        this.connection = connection;
        initialize();
    }

    /**
     * Ensures the single metadata row exists.
     */
    private void initialize() throws SQLException {

        String sql = """
                INSERT INTO syncflow_metadata (id)
                VALUES (1)
                ON CONFLICT (id) DO NOTHING
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }
    }

    /**
     * Saves the checkpoint inside the caller's current transaction.
     * Do not commit here: the checkpoint and target data must commit together.
     */
    public void saveCheckpoint(String lsn) throws SQLException {

        if (lsn == null || lsn.isBlank()) {
            throw new IllegalArgumentException("LSN cannot be empty");
        }

        String sql = """
                UPDATE syncflow_metadata
                SET last_processed_lsn = ?,
                    last_sync_time = CURRENT_TIMESTAMP
                WHERE id = 1
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {

            statement.setString(1, lsn);

            int updated = statement.executeUpdate();

            if (updated != 1) {
                throw new SQLException(
                        "Failed to update SyncFlow checkpoint");
            }
        }
    }

    /**
     * Reads the last successfully committed checkpoint from PostgreSQL.
     */
    public String getLastProcessedLsn() throws SQLException {

        String sql = """
                SELECT last_processed_lsn
                FROM syncflow_metadata
                WHERE id = 1
                """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {

            if (resultSet.next()) {
                return resultSet.getString("last_processed_lsn");
            }

            return null;
        }
    }
}
