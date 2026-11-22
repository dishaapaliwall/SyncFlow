
package com.syncflow.snapshot;

import com.syncflow.reliability.CheckpointManager;
import com.syncflow.sync.TransactionManager;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;


public class BootstrapManagerTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String TARGET_URL =
            "jdbc:postgresql://localhost:5435/syncflow_target";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String SLOT =
            "bootstrap_manager_test_slot";

    private static final Map<String, String> TABLES =
            new LinkedHashMap<>();

    static {
        TABLES.put("customers", "bootstrap_customers_test");
        TABLES.put("products", "bootstrap_products_test");
        TABLES.put("orders", "bootstrap_orders_test");
        TABLES.put("order_items", "bootstrap_order_items_test");
        TABLES.put("payments", "bootstrap_payments_test");
    }

    private record CheckpointBackup(
            String lsn,
            Timestamp syncTime,
            long totalEvents,
            long successfulEvents,
            long failedEvents
    ) {}

    @Test
    void testBootstrapOrchestration() throws Exception {
        runBootstrapTest();
    }

    private void runBootstrapTest() throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata")
        );

        Connection stateConnection = null;
        Connection checkpointConnection = null;
        CheckpointBackup originalCheckpoint = null;

        try {
            // 1. Preserve the existing Stage 2 checkpoint.
            originalCheckpoint = readCheckpoint();

            // 2. Create separate scratch tables.
            createScratchTables();

            // 3. Initialize state manager.
            stateConnection =
                    DriverManager.getConnection(
                            TARGET_URL, USER, PASSWORD
                    );

            SnapshotStateManager stateManager =
                    new SnapshotStateManager(stateConnection);

            stateManager.initialize();

            // 4. Create one shared connection for checkpoint
            // and transaction management.
            checkpointConnection =
                    DriverManager.getConnection(
                            TARGET_URL, USER, PASSWORD
                    );

            CheckpointManager checkpointManager =
                    new CheckpointManager(checkpointConnection);

            TransactionManager transactionManager =
                    new TransactionManager(checkpointConnection);

            // 5. Initialize snapshot components.
            SnapshotManager snapshotManager =
                    new SnapshotManager(
                            SOURCE_URL, USER, PASSWORD
                    );

            SnapshotWorker snapshotWorker =
                    new SnapshotWorker(
                            SOURCE_URL,
                            TARGET_URL,
                            USER,
                            PASSWORD,
                            stateManager
                    );

            // 6. Create BootstrapManager with all five dependencies.
            BootstrapManager bootstrapManager =
                    new BootstrapManager(
                            snapshotManager,
                            snapshotWorker,
                            stateManager,
                            checkpointManager,
                            transactionManager
                    );

            System.out.println("\n===== BOOTSTRAP TEST =====");

            // 7. Run bootstrap.
            bootstrapManager.startBootstrap(
                    SLOT,
                    TABLES
            );

            // 8. Verify all five copied tables.
            System.out.println("\n===== VERIFICATION =====");

            for (Map.Entry<String, String> entry :
                    TABLES.entrySet()) {

                String sourceTable = entry.getKey();
                String targetTable = entry.getValue();

                long sourceCount = getRowCount(
                        SOURCE_URL, sourceTable
                );

                long targetCount = getRowCount(
                        TARGET_URL, targetTable
                );

                System.out.println(
                        sourceTable +
                                " | Source: " + sourceCount +
                                " | Target scratch: " + targetCount
                );

                if (sourceCount != targetCount) {
                    throw new RuntimeException(
                            "Row count mismatch for " + sourceTable
                    );
                }
            }

            // 9. Verify bootstrap state.
            String status =
                    String.valueOf(stateManager.getBootstrapStatus());

            System.out.println("\nBootstrap status: " + status);

            if (!"CATCHING_UP".equals(status)) {
                throw new RuntimeException(
                        "Expected CATCHING_UP but got " + status
                );
            }

            // 10. Verify snapshot checkpoint.
            String savedLsn =
                    checkpointManager.getLastProcessedLsn();

            String snapshotLsn =
                    stateManager.getSnapshotLsn();

            System.out.println("Saved checkpoint: " + savedLsn);
            System.out.println("Snapshot LSN: " + snapshotLsn);

            if (!snapshotLsn.equals(savedLsn)) {
                throw new RuntimeException(
                        "Snapshot checkpoint does not match!"
                );
            }

            System.out.println(
                    "\nBOOTSTRAP MANAGER TEST PASSED."
            );

        } finally {

            // Release test resources and restore original metadata.
            try {
                dropTestSlot();
            } finally {
                try {
                    dropScratchTables();
                } finally {
                    try {
                        resetTestMetadata();
                    } finally {
                        try {
                            if (originalCheckpoint != null) {
                                restoreCheckpoint(originalCheckpoint);
                            }
                        } finally {
                            if (checkpointConnection != null) {
                                checkpointConnection.close();
                            }

                            if (stateConnection != null) {
                                stateConnection.close();
                            }
                        }
                    }
                }
            }
        }
    }

    private static CheckpointBackup readCheckpoint() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT last_processed_lsn,
                            last_sync_time,
                            total_events,
                            successful_events,
                            failed_events
                     FROM public.syncflow_metadata
                     WHERE id = 1
                     """)) {

            if (!rs.next()) {
                throw new IllegalStateException(
                        "Checkpoint metadata row not found."
                );
            }

            return new CheckpointBackup(
                    rs.getString("last_processed_lsn"),
                    rs.getTimestamp("last_sync_time"),
                    rs.getLong("total_events"),
                    rs.getLong("successful_events"),
                    rs.getLong("failed_events")
            );
        }
    }

    private static void restoreCheckpoint(
            CheckpointBackup backup
    ) throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             PreparedStatement ps =
                     connection.prepareStatement("""
                             UPDATE public.syncflow_metadata
                             SET last_processed_lsn = ?,
                                 last_sync_time = ?,
                                 total_events = ?,
                                 successful_events = ?,
                                 failed_events = ?
                             WHERE id = 1
                             """)) {

            ps.setString(1, backup.lsn());
            ps.setTimestamp(2, backup.syncTime());
            ps.setLong(3, backup.totalEvents());
            ps.setLong(4, backup.successfulEvents());
            ps.setLong(5, backup.failedEvents());

            ps.executeUpdate();
        }

        System.out.println(
                "[CLEANUP] Original checkpoint restored."
        );
    }

    private static void createScratchTables() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            for (Map.Entry<String, String> entry :
                    TABLES.entrySet()) {

                statement.execute(
                        "DROP TABLE IF EXISTS public.\"" +
                                entry.getValue() + "\" CASCADE"
                );

                statement.execute(
                        "CREATE TABLE public.\"" +
                                entry.getValue() +
                                "\" (LIKE public.\"" +
                                entry.getKey() +
                                "\" INCLUDING ALL)"
                );
            }
        }

        System.out.println("[TEST] Scratch tables created.");
    }

    private static long getRowCount(
            String url,
            String table
    ) throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             url, USER, PASSWORD
                     );
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT COUNT(*) FROM public.\"" +
                             table + "\""
             )) {

            rs.next();
            return rs.getLong(1);
        }
    }

    private static void dropTestSlot() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             SOURCE_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            boolean exists;

            try (ResultSet rs = statement.executeQuery(
                    "SELECT EXISTS (" +
                            "SELECT 1 FROM pg_replication_slots " +
                            "WHERE slot_name = '" + SLOT + "')"
            )) {
                rs.next();
                exists = rs.getBoolean(1);
            }

            if (exists) {
                statement.execute(
                        "SELECT pg_drop_replication_slot('" +
                                SLOT + "')"
                );

                System.out.println(
                        "[CLEANUP] Test slot dropped."
                );
            }
        }
    }

    private static void dropScratchTables() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            for (String table : TABLES.values()) {
                statement.execute(
                        "DROP TABLE IF EXISTS public.\"" +
                                table + "\" CASCADE"
                );
            }
        }

        System.out.println(
                "[CLEANUP] Scratch tables dropped."
        );
    }

    private static void resetTestMetadata() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            statement.executeUpdate(
                    "DELETE FROM public.table_state"
            );

            statement.executeUpdate("""
                    UPDATE public.bootstrap_state
                    SET status = 'NEW',
                        snapshot_lsn = NULL,
                        started_at = NULL,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                    """);
        }

        System.out.println(
                "[CLEANUP] Bootstrap metadata reset to NEW."
        );
    }
}
