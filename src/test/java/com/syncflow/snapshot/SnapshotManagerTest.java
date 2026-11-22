
package com.syncflow.snapshot;

import java.sql.*;
import java.util.TimeZone;

public class SnapshotManagerTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String TEST_SLOT =
            "snapshot_manager_test_slot";

    private static Connection normalConnection() throws SQLException {

        return DriverManager.getConnection(
                SOURCE_URL, USER, PASSWORD);
    }

    private static boolean slotExists(Connection connection)
            throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_replication_slots
                    WHERE slot_name = ?
                )
                """)) {

            statement.setString(1, TEST_SLOT);

            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static void dropTestSlot(Connection connection)
            throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_drop_replication_slot(?)")) {

            statement.setString(1, TEST_SLOT);
            statement.execute();
        }

        System.out.println("[CLEANUP] Test slot dropped");
    }

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata"));

        boolean slotCreated = false;

        SnapshotManager manager = new SnapshotManager(
                SOURCE_URL, USER, PASSWORD);

        try {

            // Never overwrite or interfere with an existing slot.
            try (Connection admin = normalConnection()) {

                if (slotExists(admin)) {
                    throw new IllegalStateException(
                            "Test slot already exists. "
                                    + "Inspect it before running again: "
                                    + TEST_SLOT);
                }
            }

            // 1. Create the permanent slot and export snapshot.
            SnapshotManager.SnapshotInfo info =
                    manager.createExportedSnapshot(TEST_SLOT);

            slotCreated = true;

            System.out.println("\nReturned snapshot information:");
            System.out.println("Slot: " + info.slotName());
            System.out.println("LSN: " + info.consistentPoint());
            System.out.println("Snapshot: " + info.snapshotName());

            // 2. Verify exporter connection is open.
            System.out.println(
                    "\nExporter open: " + manager.isExporterOpen());

            // 3. Import snapshot using another connection.
            try (Connection worker = normalConnection()) {

                worker.setTransactionIsolation(
                        Connection.TRANSACTION_REPEATABLE_READ);

                worker.setAutoCommit(false);

                try (Statement statement = worker.createStatement()) {

                    statement.execute(
                            "SET TRANSACTION SNAPSHOT '"
                                    + info.snapshotName() + "'");
                }

                System.out.println(
                        "[WORKER] Snapshot imported successfully");

                // 4. Release exporter after snapshot import.
                manager.release();

                System.out.println(
                        "Exporter open after release: "
                                + manager.isExporterOpen());

                // The imported snapshot must remain usable.
                try (Statement statement = worker.createStatement();
                     ResultSet rs = statement.executeQuery(
                             "SELECT COUNT(*) FROM pg_class")) {

                    rs.next();

                    System.out.println(
                            "[WORKER] Snapshot still usable. "
                                    + "Catalog rows: " + rs.getLong(1));
                }

                worker.rollback();
            }

            // 5. Verify permanent slot survived exporter release.
            try (Connection admin = normalConnection()) {

                boolean exists = slotExists(admin);

                System.out.println(
                        "\nPermanent slot exists after release: " + exists);

                if (!exists) {
                    throw new IllegalStateException(
                            "Permanent replication slot disappeared!");
                }
            }

            System.out.println("\nSNAPSHOT MANAGER TEST PASSED");

        } finally {

            manager.close();

            // Clean up only the dedicated test slot.
            if (slotCreated) {
                try (Connection cleanup = normalConnection()) {
                    if (slotExists(cleanup)) {
                        dropTestSlot(cleanup);
                    }
                }
            }
        }
    }
}
