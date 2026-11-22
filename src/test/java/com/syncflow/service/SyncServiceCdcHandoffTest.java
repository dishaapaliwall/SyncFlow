package com.syncflow.service;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class SyncServiceCdcHandoffTest {

    private static final String SOURCE_URL = "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String TARGET_URL = "jdbc:postgresql://localhost:5435/syncflow_target";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    // Dedicated scratch table, slot, and publication.
    // Never use production resources.
    private static final String SCRATCH_TABLE = "syncflow_handoff_test";
    private static final String TEST_SLOT = "syncflow_handoff_test_slot";
    private static final String TEST_PUBLICATION = "syncflow_handoff_pub";

    private record CheckpointBackup(
            String lsn,
            Timestamp syncTime,
            long totalEvents,
            long successfulEvents,
            long failedEvents) {
    }

    private record BootstrapStateBackup(
            String status,
            String snapshotLsn,
            Timestamp startedAt,
            Timestamp updatedAt,
            String errorMessage) {
    }

    private record TableStateBackup(
            String tableName,
            String status,
            long rowsCopied,
            Timestamp updatedAt) {
    }

    @Test
    void testInitialSnapshotAndCdcHandoff() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        SyncService service = null;
        CheckpointBackup checkpointBackup = null;
        BootstrapStateBackup bootstrapBackup = null;
        List<TableStateBackup> tableStateBackup = null;

        try {
            System.out.println("\n===== SYNC SERVICE CDC HANDOFF TEST =====");

            // 0. Backup existing metadata to safely restore after test.
            checkpointBackup = backupCheckpoint();
            bootstrapBackup = backupBootstrapState();
            tableStateBackup = backupTableState();

            // Ensure clean initial state for scratch resources.
            dropTestSlot();
            dropTestPublication();
            dropScratchTables();

            // 1. Create dedicated scratch table in source and target databases.
            // 2. Ensure table has a primary key and compatible schemas.
            createScratchTable(SOURCE_URL);
            createScratchTable(TARGET_URL);

            // 3. Insert initial row into source table before snapshot.
            insertRow(SOURCE_URL, 1, "initial_snapshot_row");
            System.out.println("[TEST] Inserted initial row into source (id=1).");

            // 4. Create dedicated publication containing only this scratch table.
            createTestPublication();
            System.out.println("[TEST] Created dedicated publication: " + TEST_PUBLICATION);

            // 5. Configure dedicated test replication slot and use dedicated publication.
            service = new SyncService(
                    SOURCE_URL,
                    USER,
                    PASSWORD,
                    TEST_SLOT,
                    TEST_PUBLICATION,
                    TARGET_URL,
                    USER,
                    PASSWORD);

            // 6. Run SyncService.bootstrapAndStart() for this table.
            System.out.println("[TEST] Running bootstrapAndStart...");
            service.bootstrapAndStart(Map.of(SCRATCH_TABLE, SCRATCH_TABLE));

            // 7. Verify that the initial row was copied to the target.
            long targetCountAfterSnapshot = getRowCount(TARGET_URL);
            assertEquals(1, targetCountAfterSnapshot, "Target should have exactly 1 row from initial snapshot");
            assertTrue(rowExists(TARGET_URL, 1, "initial_snapshot_row"),
                    "Target should contain initial snapshot row (id=1)");
            System.out.println("[TEST] Initial snapshot verified on target.");

            // Wait for CDC engine to connect.
            boolean connected = waitForConnection(service, 15000);
            assertTrue(connected, "CDC engine did not connect within 15 seconds.");
            System.out.println("[TEST] CDC engine connected.");

            // 8. Insert a second row into the source AFTER CDC engine starts.
            insertRow(SOURCE_URL, 2, "cdc_streamed_row");
            System.out.println("[TEST] Inserted second row into source AFTER CDC started (id=2).");

            // 9. Poll target database until second row appears or timeout expires.
            boolean row2Appeared = pollForTargetRow(2, "cdc_streamed_row", 20000);
            assertTrue(row2Appeared, "Second row did not replicate to target within timeout.");
            System.out.println("[TEST] Second row detected in target.");

            // 10. Verify that both rows exist exactly once in the target.
            long finalTargetCount = getRowCount(TARGET_URL);
            assertEquals(2, finalTargetCount, "Target must contain exactly 2 rows.");
            assertTrue(rowExists(TARGET_URL, 1, "initial_snapshot_row"), "Row 1 must exist in target.");
            assertTrue(rowExists(TARGET_URL, 2, "cdc_streamed_row"), "Row 2 must exist in target.");

            System.out.println("\nSYNC SERVICE CDC HANDOFF TEST PASSED.");

        } finally {
            System.out.println("[CLEANUP] Starting cleanup of test resources...");

            // Stop/close SyncService first.
            if (service != null) {
                try {
                    service.close();
                } catch (Exception e) {
                    System.err.println("[CLEANUP] Warning closing service: " + e.getMessage());
                }
            }

            // Clean up dedicated test artifacts.
            try {
                dropTestSlot();
            } catch (Exception e) {
                System.err.println("[CLEANUP] Warning dropping test slot: " + e.getMessage());
            }

            try {
                dropTestPublication();
            } catch (Exception e) {
                System.err.println("[CLEANUP] Warning dropping test publication: " + e.getMessage());
            }

            try {
                dropScratchTables();
            } catch (Exception e) {
                System.err.println("[CLEANUP] Warning dropping scratch tables: " + e.getMessage());
            }

            // Restore metadata.
            try {
                restoreMetadata(checkpointBackup, bootstrapBackup, tableStateBackup);
            } catch (Exception e) {
                System.err.println("[CLEANUP] Warning restoring metadata: " + e.getMessage());
            }

            System.out.println("[CLEANUP] Cleanup completed.");
        }
    }

    @Test
    void testInitialSnapshotAndCdcHandoffWithDifferentTableNames() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        final String SRC_MAP = "syncflow_src_mapped";
        final String TGT_MAP = "syncflow_tgt_mapped";
        final String MAPPED_SLOT = "syncflow_mapped_slot";
        final String MAPPED_PUB = "syncflow_mapped_pub";

        SyncService service = null;
        CheckpointBackup checkpointBackup = null;
        BootstrapStateBackup bootstrapBackup = null;
        List<TableStateBackup> tableStateBackup = null;

        try {
            System.out.println("\n===== SYNC SERVICE CDC HANDOFF WITH MAPPED TABLE NAMES TEST =====");

            checkpointBackup = backupCheckpoint();
            bootstrapBackup = backupBootstrapState();
            tableStateBackup = backupTableState();

            // Cleanup any stale resources
            try (Connection conn = getConnection(SOURCE_URL);
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = ? AND active = true AND active_pid IS NOT NULL")) {
                ps.setString(1, MAPPED_SLOT);
                ps.executeQuery();
            } catch (Exception ignored) {}

            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT pg_drop_replication_slot('" + MAPPED_SLOT + "')");
            } catch (Exception ignored) {}

            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP PUBLICATION IF EXISTS " + MAPPED_PUB);
            } catch (Exception ignored) {}

            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + SRC_MAP + "\" CASCADE");
            } catch (Exception ignored) {}

            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + TGT_MAP + "\" CASCADE");
            } catch (Exception ignored) {}

            // Create source and target tables with DIFFERENT names
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE public.\"" + SRC_MAP + "\" (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)");
            }
            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE public.\"" + TGT_MAP + "\" (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)");
            }

            // Publication on source for SRC_MAP
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE PUBLICATION " + MAPPED_PUB + " FOR TABLE public.\"" + SRC_MAP + "\"");
            }

            // Insert initial row into source table before snapshot
            try (Connection conn = getConnection(SOURCE_URL);
                 PreparedStatement ps = conn.prepareStatement("INSERT INTO public.\"" + SRC_MAP + "\" (id, name) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "mapped_snapshot_row");
                ps.executeUpdate();
            }

            service = new SyncService(
                    SOURCE_URL, USER, PASSWORD,
                    MAPPED_SLOT, MAPPED_PUB,
                    TARGET_URL, USER, PASSWORD
            );

            // Run bootstrapAndStart with mapping SRC_MAP -> TGT_MAP
            service.bootstrapAndStart(Map.of(SRC_MAP, TGT_MAP));

            // Verify initial snapshot copied row 1 to TGT_MAP
            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.\"" + TGT_MAP + "\"")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
            }

            // Wait for CDC engine to connect
            assertTrue(waitForConnection(service, 15000));

            // Insert second row into source table AFTER CDC started
            try (Connection conn = getConnection(SOURCE_URL);
                 PreparedStatement ps = conn.prepareStatement("INSERT INTO public.\"" + SRC_MAP + "\" (id, name) VALUES (?, ?)")) {
                ps.setInt(1, 2);
                ps.setString(2, "mapped_cdc_row");
                ps.executeUpdate();
            }

            // Poll target database table TGT_MAP until second row appears
            long deadline = System.currentTimeMillis() + 20000;
            boolean found = false;
            while (System.currentTimeMillis() < deadline) {
                try (Connection conn = getConnection(TARGET_URL);
                     PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM public.\"" + TGT_MAP + "\" WHERE id = 2 AND name = 'mapped_cdc_row'")) {
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            found = true;
                            break;
                        }
                    }
                }
                Thread.sleep(100);
            }
            assertTrue(found, "Second row did not replicate to mapped target table within timeout.");

            // Verify final target count is exactly 2
            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.\"" + TGT_MAP + "\"")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
            }

            System.out.println("SYNC SERVICE CDC HANDOFF WITH MAPPED TABLE NAMES TEST PASSED.");

        } finally {
            if (service != null) {
                try { service.close(); } catch (Exception ignored) {}
            }
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT pg_drop_replication_slot('" + MAPPED_SLOT + "')");
            } catch (Exception ignored) {}
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP PUBLICATION IF EXISTS " + MAPPED_PUB);
            } catch (Exception ignored) {}
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + SRC_MAP + "\" CASCADE");
            } catch (Exception ignored) {}
            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + TGT_MAP + "\" CASCADE");
            } catch (Exception ignored) {}

            try {
                restoreMetadata(checkpointBackup, bootstrapBackup, tableStateBackup);
            } catch (Exception ignored) {}
        }
    }

    private static Connection getConnection(String url) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("options", "-c TimeZone=UTC");
        return DriverManager.getConnection(url, properties);
    }

    private static void createScratchTable(String url) throws SQLException {
        try (Connection conn = getConnection(url);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS public.\"" + SCRATCH_TABLE + "\" CASCADE");
            stmt.execute("""
                    CREATE TABLE public."syncflow_handoff_test" (
                        id INT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }
    }

    private static void insertRow(String url, int id, String name) throws SQLException {
        try (Connection conn = getConnection(url);
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO public.\"" + SCRATCH_TABLE + "\" (id, name) VALUES (?, ?)")) {
            ps.setInt(1, id);
            ps.setString(2, name);
            ps.executeUpdate();
        }
    }

    private static void createTestPublication() throws SQLException {
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP PUBLICATION IF EXISTS " + TEST_PUBLICATION);
            stmt.execute("CREATE PUBLICATION " + TEST_PUBLICATION + " FOR TABLE public.\"" + SCRATCH_TABLE + "\"");
        }
    }

    private static void dropTestPublication() throws SQLException {
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP PUBLICATION IF EXISTS " + TEST_PUBLICATION);
        }
    }

    private static void dropScratchTables() {
        for (String url : List.of(SOURCE_URL, TARGET_URL)) {
            try (Connection conn = getConnection(url);
                    Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + SCRATCH_TABLE + "\" CASCADE");
            } catch (Exception ignored) {
            }
        }
    }

    private static void dropTestSlot() throws Exception {
        long deadline = System.currentTimeMillis() + 10000;
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {

            while (System.currentTimeMillis() < deadline) {
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT active FROM pg_replication_slots WHERE slot_name = '" + TEST_SLOT + "'")) {
                    if (!rs.next()) {
                        return; // Slot does not exist.
                    }
                    boolean active = rs.getBoolean(1);
                    if (!active) {
                        stmt.execute("SELECT pg_drop_replication_slot('" + TEST_SLOT + "')");
                        System.out.println("[CLEANUP] Dedicated test replication slot dropped.");
                        return;
                    }
                }
                Thread.sleep(200);
            }

            // Final attempt after deadline.
            stmt.execute("SELECT pg_drop_replication_slot('" + TEST_SLOT + "')");
        } catch (SQLException e) {
            // Slot may not exist.
            if (!e.getMessage().contains("does not exist")) {
                throw e;
            }
        }
    }

    private static long getRowCount(String url) throws SQLException {
        try (Connection conn = getConnection(url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.\"" + SCRATCH_TABLE + "\"")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static boolean rowExists(String url, int id, String name) throws SQLException {
        try (Connection conn = getConnection(url);
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT COUNT(*) FROM public.\"" + SCRATCH_TABLE + "\" WHERE id = ? AND name = ?")) {
            ps.setInt(1, id);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1) == 1;
            }
        }
    }

    private static boolean waitForConnection(SyncService service, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (service.isConnected()) {
                return true;
            }
            Thread.sleep(100);
        }
        return service.isConnected();
    }

    private static boolean pollForTargetRow(int id, String name, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (rowExists(TARGET_URL, id, name)) {
                return true;
            }
            Thread.sleep(100);
        }
        return rowExists(TARGET_URL, id, name);
    }

    private static CheckpointBackup backupCheckpoint() {
        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("""
                        SELECT last_processed_lsn, last_sync_time, total_events, successful_events, failed_events
                        FROM public.syncflow_metadata WHERE id = 1
                        """)) {
            if (rs.next()) {
                return new CheckpointBackup(
                        rs.getString("last_processed_lsn"),
                        rs.getTimestamp("last_sync_time"),
                        rs.getLong("total_events"),
                        rs.getLong("successful_events"),
                        rs.getLong("failed_events"));
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static BootstrapStateBackup backupBootstrapState() {
        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("""
                        SELECT status, snapshot_lsn, started_at, updated_at, error_message
                        FROM public.bootstrap_state WHERE id = 1
                        """)) {
            if (rs.next()) {
                return new BootstrapStateBackup(
                        rs.getString("status"),
                        rs.getString("snapshot_lsn"),
                        rs.getTimestamp("started_at"),
                        rs.getTimestamp("updated_at"),
                        rs.getString("error_message"));
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private static List<TableStateBackup> backupTableState() {
        List<TableStateBackup> list = new ArrayList<>();
        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("""
                        SELECT table_name, status, rows_copied, updated_at
                        FROM public.table_state
                        """)) {
            while (rs.next()) {
                list.add(new TableStateBackup(
                        rs.getString("table_name"),
                        rs.getString("status"),
                        rs.getLong("rows_copied"),
                        rs.getTimestamp("updated_at")));
            }
        } catch (Exception ignored) {
        }
        return list;
    }

    private static void restoreMetadata(
            CheckpointBackup checkpoint,
            BootstrapStateBackup bootstrap,
            List<TableStateBackup> tables) throws SQLException {
        try (Connection conn = getConnection(TARGET_URL)) {
            // Restore checkpoint.
            if (checkpoint != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                        UPDATE public.syncflow_metadata
                        SET last_processed_lsn = ?,
                            last_sync_time = ?,
                            total_events = ?,
                            successful_events = ?,
                            failed_events = ?
                        WHERE id = 1
                        """)) {
                    ps.setString(1, checkpoint.lsn());
                    ps.setTimestamp(2, checkpoint.syncTime());
                    ps.setLong(3, checkpoint.totalEvents());
                    ps.setLong(4, checkpoint.successfulEvents());
                    ps.setLong(5, checkpoint.failedEvents());
                    ps.executeUpdate();
                }
            }

            // Restore bootstrap state.
            if (bootstrap != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                        UPDATE public.bootstrap_state
                        SET status = ?,
                            snapshot_lsn = ?,
                            started_at = ?,
                            updated_at = ?,
                            error_message = ?
                        WHERE id = 1
                        """)) {
                    ps.setString(1, bootstrap.status());
                    ps.setString(2, bootstrap.snapshotLsn());
                    ps.setTimestamp(3, bootstrap.startedAt());
                    ps.setTimestamp(4, bootstrap.updatedAt());
                    ps.setString(5, bootstrap.errorMessage());
                    ps.executeUpdate();
                }
            }

            // Restore table state.
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DELETE FROM public.table_state");
            }
            if (tables != null && !tables.isEmpty()) {
                try (PreparedStatement ps = conn.prepareStatement("""
                        INSERT INTO public.table_state (table_name, status, rows_copied, updated_at)
                        VALUES (?, ?, ?, ?)
                        """)) {
                    for (TableStateBackup row : tables) {
                        ps.setString(1, row.tableName());
                        ps.setString(2, row.status());
                        ps.setLong(3, row.rowsCopied());
                        ps.setTimestamp(4, row.updatedAt());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
        }
    }
}
