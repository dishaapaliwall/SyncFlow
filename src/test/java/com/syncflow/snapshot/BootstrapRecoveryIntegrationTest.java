package com.syncflow.snapshot;

import com.syncflow.service.SyncService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

public class BootstrapRecoveryIntegrationTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";
    private static final String TARGET_URL =
            "jdbc:postgresql://localhost:5435/syncflow_target";
    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String SRC_TABLE = "restart_src_test";
    private static final String TGT_TABLE = "restart_tgt_test";
    private static final String TEST_SLOT = "restart_test_slot";
    private static final String TEST_PUB = "restart_test_pub";

    private static final String AUDIT_TABLE = "restart_audit_test";
    private static final String UNRELATED_TABLE = "restart_unrelated_tgt";
    private static final String UNRELATED_SLOT = "restart_unrelated_slot";

    // Dedicated slot, publication and scratch table for testFailedStatusWithCompletedSnapshotResumesCatchingUp.
    // Using unique, dedicated resources avoids cross-test interference and guarantees consistent table naming.
    private static final String CATCHUP_SLOT = "restart_catchup_slot";
    private static final String CATCHUP_PUB  = "restart_catchup_pub";
    private static final String CATCHUP_TABLE = "restart_catchup_test";

    private record CheckpointBackup(
            String lsn,
            Timestamp syncTime,
            long totalEvents,
            long successfulEvents,
            long failedEvents
    ) {}

    private record BootstrapStateBackup(
            String status,
            String snapshotLsn,
            Timestamp startedAt,
            Timestamp updatedAt,
            String errorMessage,
            String slotName,
            String publicationName
    ) {}

    private record TableStateBackup(
            String tableName,
            String targetTableName,
            String status,
            long rowsCopied,
            Timestamp updatedAt
    ) {}

    private CheckpointBackup checkpointBackup;
    private BootstrapStateBackup bootstrapBackup;
    private List<TableStateBackup> tableStateBackup;

    @BeforeAll
    static void initTimeZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @BeforeEach
    void setUp() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // Preserve existing production metadata before each test
        checkpointBackup = backupCheckpoint();
        bootstrapBackup = backupBootstrapState();
        tableStateBackup = backupTableState();

        cleanupTestResources();
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanupTestResources();
        restoreMetadata(checkpointBackup, bootstrapBackup, tableStateBackup);
    }

    private Connection getConnection(String url) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("options", "-c TimeZone=UTC");
        return DriverManager.getConnection(url, props);
    }

    private void cleanupTestResources() {
        // Drop test slots — includes the dedicated catch-up test slot.
        // Safely terminates active backend if a prior test thread is still connected.
        for (String slot : List.of(TEST_SLOT, CATCHUP_SLOT, UNRELATED_SLOT, "mismatched_slot", "different_configured_slot")) {
            try (Connection conn = getConnection(SOURCE_URL);
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " +
                         "WHERE slot_name = ? AND active = true AND active_pid IS NOT NULL")) {
                ps.setString(1, slot);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Thread.sleep(100);
                    }
                }
            } catch (Exception ignored) {}

            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT pg_drop_replication_slot('" + slot + "')");
            } catch (Exception ignored) {}
        }

        // Drop test publications — includes the dedicated catch-up test publication
        for (String pub : List.of(TEST_PUB, CATCHUP_PUB, "mismatched_pub", "different_configured_pub")) {
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP PUBLICATION IF EXISTS " + pub);
            } catch (Exception ignored) {}
        }

        // Drop test scratch tables on target
        for (String table : List.of(AUDIT_TABLE, TGT_TABLE, CATCHUP_TABLE, UNRELATED_TABLE, "different_tgt")) {
            try (Connection conn = getConnection(TARGET_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + table + "\" CASCADE");
            } catch (Exception ignored) {}
        }

        // Drop test scratch tables on source
        for (String table : List.of(SRC_TABLE, CATCHUP_TABLE)) {
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.\"" + table + "\" CASCADE");
            } catch (Exception ignored) {}
        }

        // Reset bootstrap metadata to a neutral NEW state so no test inherits stale slot_name,
        // publication_name, or status from a prior test.
        // Safe because @BeforeEach always backs up metadata before calling this, and @AfterEach
        // restores the backup after — so production metadata is fully preserved across tests.
        try (Connection conn = getConnection(TARGET_URL);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                "UPDATE bootstrap_state " +
                "SET status = 'NEW', slot_name = NULL, publication_name = NULL, " +
                "    snapshot_lsn = NULL, error_message = NULL, " +
                "    updated_at = CURRENT_TIMESTAMP " +
                "WHERE id = 1"
            );
            stmt.executeUpdate("DELETE FROM table_state");
        } catch (Exception ignored) {}
    }

    private void createTable(String url, String table) throws SQLException {
        try (Connection conn = getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE public.\"" + table + "\" (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)");
        }
    }

    private void createScratchTables() throws SQLException {
        try (Connection srcConn = getConnection(SOURCE_URL);
             Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."restart_src_test" (
                        id INT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
             Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."restart_tgt_test" (
                        id INT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }
    }

    private void createPublication() throws SQLException {
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PUBLICATION " + TEST_PUB + " FOR TABLE public.\"" + SRC_TABLE + "\"");
        }
    }

    private void insertRow(String url, String table, int id, String name) throws SQLException {
        try (Connection conn = getConnection(url);
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO public.\"" + table + "\" (id, name) VALUES (?, ?)")) {
            ps.setInt(1, id);
            ps.setString(2, name);
            ps.executeUpdate();
        }
    }

    private long getRowCount(String url, String table) throws SQLException {
        try (Connection conn = getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM public.\"" + table + "\"")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private boolean rowExists(String url, String table, int id, String name) throws SQLException {
        try (Connection conn = getConnection(url);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM public.\"" + table + "\" WHERE id = ? AND name = ?")) {
            ps.setInt(1, id);
            ps.setString(2, name);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1) == 1;
            }
        }
    }

    private boolean waitForConnection(SyncService service, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (service.isConnected()) {
                return true;
            }
            Thread.sleep(100);
        }
        return service.isConnected();
    }

    private boolean pollForTargetRow(String table, int id, String name, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (rowExists(TARGET_URL, table, id, name)) {
                return true;
            }
            Thread.sleep(100);
        }
        return rowExists(TARGET_URL, table, id, name);
    }

    // =========================================================================
    // FIX 1 TEST: TRUNCATE RESTRICT protects external referencing table
    // =========================================================================

    @Test
    void testTruncateRestrictProtectsExternalReferencingTable() throws Exception {
        System.out.println("\n=== TEST: TRUNCATE RESTRICT protects external referencing table ===");
        createScratchTables();
        createPublication();

        // 1. Target table has a record
        insertRow(TARGET_URL, TGT_TABLE, 1, "managed_row");

        // 2. Create an external audit table referencing the managed target table
        try (Connection conn = getConnection(TARGET_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."restart_audit_test" (
                        audit_id INT PRIMARY KEY,
                        ref_id INT NOT NULL REFERENCES public."restart_tgt_test"(id)
                    )
                    """);
            stmt.execute("INSERT INTO public.\"restart_audit_test\" (audit_id, ref_id) VALUES (10, 1)");
        }

        // 3. Stale slot exists and is recorded
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_create_logical_replication_slot('" + TEST_SLOT + "', 'pgoutput')");
        }

        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager stateManager = new SnapshotStateManager(conn);
            stateManager.initialize();
            stateManager.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            stateManager.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
        }

        // 4. Attempt incomplete snapshot recovery
        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, TEST_PUB,
                TARGET_URL, USER, PASSWORD
        )) {
            // Recovery must fail safely with an exception indicating external FK reference prevents truncation
            Exception ex = assertThrows(Exception.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));

            assertTrue(ex.getMessage().contains("external or unregistered table") || ex.getMessage().contains("foreign key"),
                    "Expected safe FK violation message but got: " + ex.getMessage());

            // 5. Assert BOTH tables retain their original records!
            assertEquals(1, getRowCount(TARGET_URL, TGT_TABLE), "Managed table must NOT be truncated!");
            assertEquals(1, getRowCount(TARGET_URL, AUDIT_TABLE), "External audit table must NOT be truncated!");
            assertTrue(rowExists(TARGET_URL, TGT_TABLE, 1, "managed_row"));
        }
    }

    // =========================================================================
    // FIX 2 TESTS: FAILED-state classification
    // =========================================================================

    @Test
    void testFailedStatusWithCompletedSnapshotResumesCatchingUp() throws Exception {
        System.out.println("\n=== TEST: FAILED status with completed snapshot resumes CATCHING_UP ===");

        // Dedicated slot, publication and tables for complete test isolation.
        // Terminate any active connection and drop leftover slot from prior test run if any.
        try (Connection conn = getConnection(SOURCE_URL);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " +
                     "WHERE slot_name = ? AND active = true AND active_pid IS NOT NULL")) {
            ps.setString(1, CATCHUP_SLOT);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Thread.sleep(100);
                }
            }
        } catch (Exception ignored) {}

        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_drop_replication_slot('" + CATCHUP_SLOT + "')");
        } catch (Exception ignored) {}

        // Create dedicated scratch table with consistent naming on both source and target
        createTable(SOURCE_URL, CATCHUP_TABLE);
        createTable(TARGET_URL, CATCHUP_TABLE);

        // Dedicated publication for this test's scratch table
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP PUBLICATION IF EXISTS " + CATCHUP_PUB);
            stmt.execute("CREATE PUBLICATION " + CATCHUP_PUB + " FOR TABLE public.\"" + CATCHUP_TABLE + "\"");
        }

        insertRow(SOURCE_URL, CATCHUP_TABLE, 1, "initial_snapshot_row");

        Map<String, String> tables = Map.of(CATCHUP_TABLE, CATCHUP_TABLE);

        // --- Phase 1: Run a complete, successful initial bootstrap using CATCHUP_SLOT. ---
        // This creates the real logical replication slot on the source DB via
        // SnapshotManager.createExportedSnapshot(CATCHUP_SLOT). The slot persists after stop().
        SyncService service1 = null;
        try {
            service1 = new SyncService(
                    SOURCE_URL, USER, PASSWORD,
                    CATCHUP_SLOT, CATCHUP_PUB,
                    TARGET_URL, USER, PASSWORD
            );
            service1.bootstrapAndStart(tables);
            assertTrue(waitForConnection(service1, 15000),
                    "service1: CDC should connect after fresh bootstrap");
            assertEquals(1, getRowCount(TARGET_URL, CATCHUP_TABLE),
                    "service1: snapshot row must be present in target");
            service1.stop();
        } finally {
            if (service1 != null) service1.close();
        }

        // --- Verify slot was created and still exists after service1 stopped ---
        // Closing the replication stream does NOT drop a logical replication slot in PostgreSQL.
        try (Connection conn = getConnection(SOURCE_URL);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT slot_name FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, CATCHUP_SLOT);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(),
                        "Slot '" + CATCHUP_SLOT + "' must exist on source DB after service1 stopped");
            }
        }

        // --- Phase 2: Simulate a transient failure that happened during CDC catch-up. ---
        // We only flip the status flag; we do NOT touch the slot, publication,
        // checkpoint, or table_state — they all remain consistent from Phase 1.
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.markFailed("Transient network glitch during catch-up");
            assertEquals("FAILED", sm.getBootstrapStatus(),
                    "Bootstrap status must be FAILED after simulation");
        }

        // --- Insert a row while service is offline (will be picked up via CDC catch-up) ---
        insertRow(SOURCE_URL, CATCHUP_TABLE, 2, "cdc_offline_row");

        // --- Phase 3: Restart service. Recovery must return RESUME_CATCHING_UP. ---
        // RecoveryManager inspects:
        //   - table_state: restart_catchup_test = COPIED
        //   - checkpoint LSN >= snapshot LSN (both set during Phase 1)
        //   - slot CATCHUP_SLOT exists on source DB (verified above)
        //   - slot_name in bootstrap_state = CATCHUP_SLOT (recorded by BootstrapManager)
        //   - publication_name in bootstrap_state = CATCHUP_PUB
        // Recovery must NOT truncate target tables or repeat the snapshot.
        SyncService service2 = null;
        try {
            service2 = new SyncService(
                    SOURCE_URL, USER, PASSWORD,
                    CATCHUP_SLOT, CATCHUP_PUB,
                    TARGET_URL, USER, PASSWORD
            );
            service2.bootstrapAndStart(tables);
            assertTrue(waitForConnection(service2, 15000),
                    "service2: CDC should reconnect to existing slot for catch-up");

            // Row 2 (inserted while offline) must be received through CDC catch-up
            assertTrue(pollForTargetRow(CATCHUP_TABLE, 2, "cdc_offline_row", 20000),
                    "CDC catch-up must deliver the row inserted while service was offline");

            // Exactly 2 rows — initial snapshot row was NOT deleted or duplicated
            assertEquals(2, getRowCount(TARGET_URL, CATCHUP_TABLE),
                    "Target must have exactly 2 rows: snapshot row + CDC catch-up row");
            assertTrue(rowExists(TARGET_URL, CATCHUP_TABLE, 1, "initial_snapshot_row"),
                    "Snapshot row must still exist (not truncated by recovery)");
            assertTrue(rowExists(TARGET_URL, CATCHUP_TABLE, 2, "cdc_offline_row"),
                    "CDC catch-up row must exist in target");

            // Slot must still exist (recovery resumed from it; did not drop it)
            try (Connection conn = getConnection(SOURCE_URL);
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT slot_name FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, CATCHUP_SLOT);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(),
                            "Slot must remain present after RESUME_CATCHING_UP recovery");
                }
            }

        } finally {
            if (service2 != null) service2.close();
        }
    }

    @Test
    void testFailedStatusWithIncompleteSnapshotRetriesSnapshot() throws Exception {
        System.out.println("\n=== TEST: FAILED status with incomplete snapshot retries snapshot ===");
        createScratchTables();
        createPublication();

        insertRow(SOURCE_URL, SRC_TABLE, 1, "source_row_1");
        insertRow(SOURCE_URL, SRC_TABLE, 2, "source_row_2");

        // Target has dirty row 999 from failed attempt
        insertRow(TARGET_URL, TGT_TABLE, 999, "dirty_row");

        // Slot exists on source and ownership is recorded
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_create_logical_replication_slot('" + TEST_SLOT + "', 'pgoutput')");
        }

        // Status is FAILED and table_state has FAILED table
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            sm.markTableFailed(SRC_TABLE);
            sm.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
            sm.markFailed("Copy aborted mid-flight");
        }

        // Restart service: should detect demonstrably incomplete snapshot, reset dirty row, and retry snapshot
        SyncService service = null;
        try {
            service = new SyncService(
                    SOURCE_URL, USER, PASSWORD,
                    TEST_SLOT, TEST_PUB,
                    TARGET_URL, USER, PASSWORD
            );
            service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE));

            // Dirty row 999 is gone, fresh snapshot rows 1 and 2 exist
            assertFalse(rowExists(TARGET_URL, TGT_TABLE, 999, "dirty_row"));
            assertEquals(2, getRowCount(TARGET_URL, TGT_TABLE));
            assertTrue(rowExists(TARGET_URL, TGT_TABLE, 1, "source_row_1"));
            assertTrue(rowExists(TARGET_URL, TGT_TABLE, 2, "source_row_2"));

        } finally {
            if (service != null) service.close();
        }
    }

    // =========================================================================
    // FIX 3 TEST: Strengthened replication slot ownership
    // =========================================================================

    @Test
    void testUnrecordedSlotNotDroppedOnRecovery() throws Exception {
        System.out.println("\n=== TEST: Unrecorded slot not dropped on recovery ===");
        createScratchTables();
        createPublication();

        // Target table has dirty row
        insertRow(TARGET_URL, TGT_TABLE, 100, "stale_row");

        // Slot exists on PostgreSQL
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_create_logical_replication_slot('" + TEST_SLOT + "', 'pgoutput')");
        }

        // Status is COPYING, but slot_name was NEVER recorded in bootstrap_state
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            // Explicitly ensure slot_name is NULL
            try (Statement s = conn.createStatement()) {
                s.executeUpdate("UPDATE bootstrap_state SET slot_name = NULL WHERE id = 1");
            }
        }

        // Restart recovery must fail explicitly without dropping the unverified slot
        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, TEST_PUB,
                TARGET_URL, USER, PASSWORD
        )) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));

            assertTrue(ex.getMessage().contains("no recorded slot ownership"),
                    "Expected unverified slot ownership error but got: " + ex.getMessage());

            // Verify slot was NOT deleted from PostgreSQL!
            try (Connection conn = getConnection(SOURCE_URL);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + TEST_SLOT + "'")) {
                assertTrue(rs.next(), "Unrecorded slot must NOT be deleted!");
            }

            // Verify target table was NOT truncated!
            assertEquals(1, getRowCount(TARGET_URL, TGT_TABLE));
        }
    }

    // =========================================================================
    // FIX 4 TESTS: Validate configuration consistency
    // =========================================================================

    @Test
    void testConfiguredSlotMismatchRejectsRecovery() throws Exception {
        System.out.println("\n=== TEST: Slot mismatch rejects recovery ===");
        createScratchTables();
        createPublication();

        // Persist CATCHING_UP with slot "restart_test_slot"
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            sm.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
            sm.markCatchingUp();
        }

        // Configure service with "different_configured_slot"
        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                "different_configured_slot", TEST_PUB,
                TARGET_URL, USER, PASSWORD
        )) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));
            assertTrue(ex.getMessage().contains("does not match persisted bootstrap slot"),
                    "Expected slot mismatch error but got: " + ex.getMessage());
        }
    }

    @Test
    void testPublicationMismatchRejectsRecovery() throws Exception {
        System.out.println("\n=== TEST: Publication mismatch rejects recovery ===");
        createScratchTables();
        createPublication();

        // Persist CATCHING_UP with pub "restart_test_pub"
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            sm.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
            sm.markCatchingUp();
        }

        // Configure service with "different_configured_pub"
        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, "different_configured_pub",
                TARGET_URL, USER, PASSWORD
        )) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));
            assertTrue(ex.getMessage().contains("does not match persisted bootstrap publication"),
                    "Expected publication mismatch error but got: " + ex.getMessage());
        }
    }

    @Test
    void testTableMappingMismatchRejectsRecovery() throws Exception {
        System.out.println("\n=== TEST: Table mapping mismatch rejects recovery ===");
        createScratchTables();
        createPublication();

        // Persist CATCHING_UP with mapping SRC_TABLE -> TGT_TABLE
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/1000", Map.of(SRC_TABLE, TGT_TABLE));
            sm.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
            sm.markCatchingUp();
        }

        // Call bootstrapAndStart with mapping SRC_TABLE -> "different_tgt"
        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, TEST_PUB,
                TARGET_URL, USER, PASSWORD
        )) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, "different_tgt")));
            assertTrue(ex.getMessage().contains("does not match persisted bootstrap table mapping"),
                    "Expected table mapping mismatch error but got: " + ex.getMessage());
        }
    }

    // =========================================================================
    // FIX 5 TEST: Checkpoint earlier than snapshot LSN rejected
    // =========================================================================

    @Test
    void testCheckpointEarlierThanSnapshotLsnRejected() throws Exception {
        System.out.println("\n=== TEST: Checkpoint earlier than snapshot LSN rejected ===");
        createScratchTables();
        createPublication();

        // Create slot
        try (Connection conn = getConnection(SOURCE_URL);
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_create_logical_replication_slot('" + TEST_SLOT + "', 'pgoutput')");
        }

        // Persist CATCHING_UP with snapshot LSN 0/2000, but checkpoint in metadata has older LSN 0/1000
        try (Connection conn = getConnection(TARGET_URL)) {
            SnapshotStateManager sm = new SnapshotStateManager(conn);
            sm.initialize();
            sm.beginBootstrap("0/2000", Map.of(SRC_TABLE, TGT_TABLE));
            sm.recordSlotAndPublication(TEST_SLOT, TEST_PUB);
            sm.markCatchingUp();

            try (Statement s = conn.createStatement()) {
                s.executeUpdate("UPDATE syncflow_metadata SET last_processed_lsn = '0/1000' WHERE id = 1");
            }
        }

        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, TEST_PUB,
                TARGET_URL, USER, PASSWORD
        )) {
            IllegalStateException ex = assertThrows(IllegalStateException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));
            assertTrue(ex.getMessage().contains("is earlier than snapshot LSN"),
                    "Expected earlier checkpoint rejection but got: " + ex.getMessage());
        }
    }

    // =========================================================================
    // METADATA BACKUP & RESTORATION HELPERS
    // =========================================================================

    private CheckpointBackup backupCheckpoint() {
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
                        rs.getLong("failed_events")
                );
            }
        } catch (Exception ignored) {}
        return null;
    }

    private BootstrapStateBackup backupBootstrapState() {
        try (Connection conn = getConnection(TARGET_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("""
                     SELECT status, snapshot_lsn, started_at, updated_at, error_message, slot_name, publication_name
                     FROM public.bootstrap_state WHERE id = 1
                     """)) {
            if (rs.next()) {
                return new BootstrapStateBackup(
                        rs.getString("status"),
                        rs.getString("snapshot_lsn"),
                        rs.getTimestamp("started_at"),
                        rs.getTimestamp("updated_at"),
                        rs.getString("error_message"),
                        rs.getString("slot_name"),
                        rs.getString("publication_name")
                );
            }
        } catch (Exception ignored) {}
        return null;
    }

    private List<TableStateBackup> backupTableState() {
        List<TableStateBackup> list = new ArrayList<>();
        try (Connection conn = getConnection(TARGET_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("""
                     SELECT table_name, target_table_name, status, rows_copied, updated_at
                     FROM public.table_state
                     """)) {
            while (rs.next()) {
                list.add(new TableStateBackup(
                        rs.getString("table_name"),
                        rs.getString("target_table_name"),
                        rs.getString("status"),
                        rs.getLong("rows_copied"),
                        rs.getTimestamp("updated_at")
                ));
            }
        } catch (Exception ignored) {}
        return list;
    }

    private void restoreMetadata(
            CheckpointBackup checkpoint,
            BootstrapStateBackup bootstrap,
            List<TableStateBackup> tables
    ) throws SQLException {
        try (Connection conn = getConnection(TARGET_URL)) {
            // Restore checkpoint
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

            // Restore bootstrap state
            if (bootstrap != null) {
                try (PreparedStatement ps = conn.prepareStatement("""
                        UPDATE public.bootstrap_state
                        SET status = ?,
                            snapshot_lsn = ?,
                            started_at = ?,
                            updated_at = ?,
                            error_message = ?,
                            slot_name = ?,
                            publication_name = ?
                        WHERE id = 1
                        """)) {
                    ps.setString(1, bootstrap.status());
                    ps.setString(2, bootstrap.snapshotLsn());
                    ps.setTimestamp(3, bootstrap.startedAt());
                    ps.setTimestamp(4, bootstrap.updatedAt());
                    ps.setString(5, bootstrap.errorMessage());
                    ps.setString(6, bootstrap.slotName());
                    ps.setString(7, bootstrap.publicationName());
                    ps.executeUpdate();
                }
            }

            // Restore table state
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DELETE FROM public.table_state");
            }
            if (tables != null && !tables.isEmpty()) {
                try (PreparedStatement ps = conn.prepareStatement("""
                        INSERT INTO public.table_state (table_name, target_table_name, status, rows_copied, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        """)) {
                    for (TableStateBackup row : tables) {
                        ps.setString(1, row.tableName());
                        ps.setString(2, row.targetTableName());
                        ps.setString(3, row.status());
                        ps.setLong(4, row.rowsCopied());
                        ps.setTimestamp(5, row.updatedAt());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
        }
    }
}
