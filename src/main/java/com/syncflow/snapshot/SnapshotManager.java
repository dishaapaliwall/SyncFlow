
package com.syncflow.snapshot;

import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Pattern;

public class SnapshotManager implements AutoCloseable {

    private static final Pattern VALID_SLOT_NAME =
            Pattern.compile("[a-z0-9_]{1,63}");

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private Connection exporterConnection;

    public SnapshotManager(
            String jdbcUrl,
            String username,
            String password) {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * Information returned by PostgreSQL when exporting a snapshot.
     */
    public record SnapshotInfo(
            String slotName,
            String consistentPoint,
            String snapshotName) {
    }

    /**
     * Creates a permanent logical replication slot
     * and exports a consistent database snapshot.
     *
     * The exporter connection stays open until release() is called.
     */
    public synchronized SnapshotInfo createExportedSnapshot(
            String slotName) throws SQLException {

        if (exporterConnection != null
                && !exporterConnection.isClosed()) {

            throw new IllegalStateException(
                    "An exported snapshot is already active");
        }

        if (slotName == null
                || !VALID_SLOT_NAME.matcher(slotName).matches()) {

            throw new IllegalArgumentException(
                    "Invalid replication slot name: " + slotName);
        }

        Properties properties = new Properties();

        properties.setProperty("user", username);
        properties.setProperty("password", password);

        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "12");

        Connection connection =
                DriverManager.getConnection(jdbcUrl, properties);

        try {
            String sql = "CREATE_REPLICATION_SLOT "
                    + slotName
                    + " LOGICAL pgoutput (SNAPSHOT 'export')";

            String consistentPoint;
            String snapshotName;

            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {

                if (!rs.next()) {
                    throw new SQLException(
                            "PostgreSQL did not return snapshot information");
                }

                consistentPoint =
                        rs.getString("consistent_point");

                snapshotName =
                        rs.getString("snapshot_name");
            }

            if (consistentPoint == null || snapshotName == null) {
                throw new SQLException(
                        "PostgreSQL returned incomplete snapshot information");
            }

            // Keep this connection open.
            // Workers must import the exported snapshot before release().
            this.exporterConnection = connection;

            System.out.println(
                    "[SNAPSHOT] Slot created: " + slotName);

            System.out.println(
                    "[SNAPSHOT] Consistent point: " + consistentPoint);

            System.out.println(
                    "[SNAPSHOT] Snapshot name: " + snapshotName);

            return new SnapshotInfo(
                    slotName,
                    consistentPoint,
                    snapshotName);

        } catch (SQLException | RuntimeException e) {

            try {
                connection.close();
            } catch (SQLException closeError) {
                e.addSuppressed(closeError);
            }

            throw e;
        }
    }

    /**
     * Releases the exporting connection after all workers
     * have imported the exported snapshot.
     *
     * The permanent replication slot is NOT dropped.
     */
    public synchronized void release() throws SQLException {

        if (exporterConnection != null) {
            exporterConnection.close();
            exporterConnection = null;

            System.out.println(
                    "[SNAPSHOT] Exporter connection released");
        }
    }

    public synchronized boolean isExporterOpen()
            throws SQLException {

        return exporterConnection != null
                && !exporterConnection.isClosed();
    }

    public synchronized boolean slotExists(String slotName) throws SQLException {
        if (slotName == null || !VALID_SLOT_NAME.matcher(slotName).matches()) {
            return false;
        }

        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("options", "-c TimeZone=UTC");
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             PreparedStatement ps = connection.prepareStatement(
                     "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public synchronized void dropSlot(String slotName) throws SQLException {
        if (slotName == null || !VALID_SLOT_NAME.matcher(slotName).matches()) {
            throw new IllegalArgumentException(
                    "Invalid replication slot name: " + slotName);
        }

        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("options", "-c TimeZone=UTC");
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            System.out.println("[SNAPSHOT] Replication slot dropped: " + slotName);
        }
    }

    /**
     * Closing SnapshotManager releases only the exporter connection.
     * It does not delete the permanent replication slot.
     */
    @Override
    public void close() throws SQLException {
        release();
    }
}
