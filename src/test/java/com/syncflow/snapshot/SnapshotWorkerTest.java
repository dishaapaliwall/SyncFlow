
package com.syncflow.snapshot;

import java.sql.*;
import java.util.List;
import java.util.TimeZone;

public class SnapshotWorkerTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String TARGET_URL =
            "jdbc:postgresql://localhost:5435/syncflow_target";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String TEST_SLOT =
            "snapshot_worker_test_slot";

    private static final String TEST_TABLE =
            "snapshot_customers_test";

    private static Connection connect(String url) throws SQLException {
        return DriverManager.getConnection(url, USER, PASSWORD);
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

    private static long countRows(
            Connection connection,
            String tableName) throws SQLException {

        // Table name is a fixed test constant, not user input.
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT COUNT(*) FROM public.\""
                             + tableName + "\"")) {

            rs.next();
            return rs.getLong(1);
        }
    }

    private static void cleanupSlot(Connection connection)
            throws SQLException {

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_drop_replication_slot(?)")) {

            statement.setString(1, TEST_SLOT);
            statement.execute();
        }

        System.out.println("[CLEANUP] Test replication slot dropped");
    }

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata"));

        boolean slotCreated = false;
        boolean scratchCreated = false;

        SnapshotManager snapshotManager = new SnapshotManager(
                SOURCE_URL, USER, PASSWORD);

        try {

            // 1. Prepare the target scratch table.
            try (Connection target = connect(TARGET_URL);
                 Statement statement = target.createStatement()) {

                statement.execute(
                        "DROP TABLE IF EXISTS public.\""
                                + TEST_TABLE + "\"");

                statement.execute(
                        "CREATE TABLE public.\""
                                + TEST_TABLE
                                + "\" (LIKE public.customers INCLUDING ALL)");

                scratchCreated = true;
            }

            System.out.println("[TEST] Target scratch table created");

            // 2. Ensure our dedicated test slot does not already exist.
            try (Connection source = connect(SOURCE_URL)) {

                if (slotExists(source)) {
                    throw new IllegalStateException(
                            "Test slot already exists. Inspect it first: "
                                    + TEST_SLOT);
                }
            }

            // 3. Export a real source database snapshot.
            SnapshotManager.SnapshotInfo snapshot =
                    snapshotManager.createExportedSnapshot(TEST_SLOT);

            slotCreated = true;

            // 4. Initialize and register the customer table.
            try (Connection target = connect(TARGET_URL)) {

                SnapshotStateManager stateManager =
                        new SnapshotStateManager(target);

                stateManager.initialize();

                stateManager.beginBootstrap(
                        snapshot.consistentPoint(),
                        List.of("customers"));

                // 5. Copy actual source customers using the snapshot.
                SnapshotWorker worker = new SnapshotWorker(
                        SOURCE_URL,
                        TARGET_URL,
                        USER,
                        PASSWORD,
                        stateManager);

                long copied = worker.copyTable(
                        snapshot.snapshotName(),
                        "customers",
                        TEST_TABLE);

                // 6. Verify target row count.
                long targetCount = countRows(target, TEST_TABLE);

                System.out.println("\n===== VERIFICATION =====");

                System.out.println("Snapshot LSN: "
                        + snapshot.consistentPoint());

                System.out.println("Rows reported by worker: "
                        + copied);

                System.out.println("Rows in target scratch table: "
                        + targetCount);

                System.out.println("Bootstrap status: "
                        + stateManager.getBootstrapStatus());

                if (copied != targetCount) {
                    throw new IllegalStateException(
                            "Copied row count does not match target count");
                }

                if (copied == 0) {
                    throw new IllegalStateException(
                            "No customer rows were copied");
                }

                System.out.println(
                        "\nSNAPSHOT WORKER TEST PASSED");
            }

        } finally {

            // 7. Release exporter before dropping its permanent slot.
            snapshotManager.close();

            // 8. Clean up only our dedicated test slot.
            if (slotCreated) {
                try (Connection source = connect(SOURCE_URL)) {
                    if (slotExists(source)) {
                        cleanupSlot(source);
                    }
                }
            }

            // 9. Remove the scratch table and test metadata.
            try (Connection target = connect(TARGET_URL);
                 Statement statement = target.createStatement()) {

                if (scratchCreated) {
                    statement.execute(
                            "DROP TABLE IF EXISTS public.\""
                                    + TEST_TABLE + "\"");

                    System.out.println(
                            "[CLEANUP] Target scratch table dropped");
                }

                statement.executeUpdate(
                        "DELETE FROM table_state");

                statement.executeUpdate("""
                        UPDATE bootstrap_state
                        SET status = 'NEW',
                            snapshot_lsn = NULL,
                            started_at = NULL,
                            error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = 1
                        """);

                System.out.println(
                        "[CLEANUP] Test metadata reset to NEW");
            }
        }
    }
}
