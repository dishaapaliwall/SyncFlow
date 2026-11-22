
package com.syncflow.service;

import com.syncflow.snapshot.*;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class SyncServiceBootstrapTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String TARGET_URL =
            "jdbc:postgresql://localhost:5435/syncflow_target";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    // Dedicated test slot. Never use the production slot here.
    private static final String TEST_SLOT =
            "syncflow_service_test_slot";

    private static final Map<String, String> TABLES =
            new LinkedHashMap<>();

    static {
        TABLES.put("customers", "service_customers_test");
        TABLES.put("products", "service_products_test");
        TABLES.put("orders", "service_orders_test");
        TABLES.put("order_items", "service_order_items_test");
        TABLES.put("payments", "service_payments_test");
    }

    private record CheckpointBackup(
            String lsn,
            Timestamp syncTime,
            long totalEvents,
            long successfulEvents,
            long failedEvents
    ) {}

    @Test
    void testServiceBootstrapAndCdcStartup() throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata")
        );

        SyncService service = null;
        CheckpointBackup backup = null;

        try {
            System.out.println("\n===== SERVICE INTEGRATION TEST =====");

            // 1. Prepare isolated target scratch tables.
            createScratchTables();

            // 2. Preserve the existing production checkpoint.
            backup = readCheckpoint();

            // 3. Instantiate the actual service.
            service = new SyncService(
                    SOURCE_URL,
                    USER,
                    PASSWORD,
                    TEST_SLOT,
                    "syncflow_publication",
                    TARGET_URL,
                    USER,
                    PASSWORD
            );

            // 4. Run snapshot bootstrap and start CDC.
            service.bootstrapAndStart(TABLES);

            System.out.println(
                    "[TEST] Bootstrap method returned."
            );

            // 5. Verify every scratch table was populated.
            for (Map.Entry<String, String> entry :
                    TABLES.entrySet()) {

                long sourceCount = getRowCount(
                        SOURCE_URL, entry.getKey()
                );

                long targetCount = getRowCount(
                        TARGET_URL, entry.getValue()
                );

                System.out.println(
                        entry.getKey()
                                + " | Source: " + sourceCount
                                + " | Scratch target: " + targetCount
                );

                assertEquals(
                        sourceCount,
                        targetCount,
                        "Row count mismatch for " + entry.getKey()
                );
            }

            // 6. Verify checkpoint equals snapshot LSN.
            String checkpoint = readCheckpointLsn();
            String snapshotLsn = readSnapshotLsn();

            assertNotNull(snapshotLsn);
            assertEquals(snapshotLsn, checkpoint);

            System.out.println(
                    "[TEST] Snapshot checkpoint verified: "
                            + checkpoint
            );

            // 7. Verify bootstrap state.
            assertEquals(
                    "CATCHING_UP",
                    readBootstrapStatus()
            );

            // 8. Wait for the CDC engine to connect.
            boolean connected = waitForConnection(service, 15000);

            assertTrue(
                    connected,
                    "CDC engine did not connect within 15 seconds."
            );

            System.out.println(
                    "[TEST] CDC engine connected successfully."
            );

            System.out.println(
                    "\nSERVICE INTEGRATION TEST PASSED."
            );

        } finally {

            // Stop CDC before dropping its replication slot.
            if (service != null) {
                service.stop();
            }

            dropTestSlot();
            dropScratchTables();
            resetBootstrapMetadata();

            // Restore the checkpoint exactly as it was before the test.
            if (backup != null) {
                restoreCheckpoint(backup);
            }

            System.out.println(
                    "[CLEANUP] Integration test cleanup completed."
            );
        }
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
                        "DROP TABLE IF EXISTS public.\""
                                + entry.getValue()
                                + "\" CASCADE"
                );

                statement.execute(
                        "CREATE TABLE public.\""
                                + entry.getValue()
                                + "\" (LIKE public.\""
                                + entry.getKey()
                                + "\" INCLUDING ALL)"
                );
            }
        }

        System.out.println("[TEST] Scratch tables created.");
    }

    private static CheckpointBackup readCheckpoint()
            throws Exception {

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

            assertTrue(rs.next(), "Checkpoint row not found.");

            return new CheckpointBackup(
                    rs.getString("last_processed_lsn"),
                    rs.getTimestamp("last_sync_time"),
                    rs.getLong("total_events"),
                    rs.getLong("successful_events"),
                    rs.getLong("failed_events")
            );
        }
    }

    private static String readCheckpointLsn()
            throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT last_processed_lsn
                     FROM public.syncflow_metadata
                     WHERE id = 1
                     """)) {

            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static String readSnapshotLsn()
            throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT snapshot_lsn
                     FROM public.bootstrap_state
                     WHERE id = 1
                     """)) {

            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static String readBootstrapStatus()
            throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("""
                     SELECT status
                     FROM public.bootstrap_state
                     WHERE id = 1
                     """)) {

            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static boolean waitForConnection(
            SyncService service,
            long timeoutMillis
    ) throws InterruptedException {

        long deadline =
                System.currentTimeMillis() + timeoutMillis;

        while (System.currentTimeMillis() < deadline) {

            if (service.isConnected()) {
                return true;
            }

            Thread.sleep(100);
        }

        return service.isConnected();
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
                     "SELECT COUNT(*) FROM public.\""
                             + table + "\""
             )) {

            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static void dropTestSlot() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             SOURCE_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            try (ResultSet rs = statement.executeQuery(
                    "SELECT EXISTS (" +
                            "SELECT 1 FROM pg_replication_slots " +
                            "WHERE slot_name = '" + TEST_SLOT + "')"
            )) {

                rs.next();

                if (rs.getBoolean(1)) {
                    statement.execute(
                            "SELECT pg_drop_replication_slot('"
                                    + TEST_SLOT + "')"
                    );

                    System.out.println(
                            "[CLEANUP] Test replication slot dropped."
                    );
                }
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
                        "DROP TABLE IF EXISTS public.\""
                                + table + "\" CASCADE"
                );
            }
        }

        System.out.println("[CLEANUP] Scratch tables dropped.");
    }

    private static void resetBootstrapMetadata()
            throws Exception {

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
                        slot_name = NULL,
                        publication_name = NULL,
                        started_at = NULL,
                        error_message = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                    """);
        }

        System.out.println(
                "[CLEANUP] Bootstrap metadata reset."
        );
    }

    private static void restoreCheckpoint(
            CheckpointBackup backup
    ) throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             PreparedStatement ps = connection.prepareStatement("""
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
}
