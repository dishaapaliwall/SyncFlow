package com.syncflow.benchmark;

import com.syncflow.snapshot.SnapshotManager;
import com.syncflow.snapshot.SnapshotStateManager;
import com.syncflow.snapshot.SnapshotWorker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark and integration test verifying that SyncFlow works with 100,000+ rows.
 * Validates bulk snapshotting, JDBC batching performance, and data integrity.
 */
public class LargeScaleSyncTest {

    private static final String SOURCE_URL = "jdbc:postgresql://localhost:5434/syncflow_source";
    private static final String TARGET_URL = "jdbc:postgresql://localhost:5435/syncflow_target";
    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String BENCHMARK_SLOT = "syncflow_100k_benchmark_slot";
    private static final String TARGET_TABLE = "customers_100k_benchmark";

    private Connection sourceConnection;
    private Connection targetConnection;
    private SnapshotManager snapshotManager;

    private Properties createProperties() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("options", "-c TimeZone=UTC");
        return props;
    }

    @BeforeEach
    void setUp() throws SQLException {
        sourceConnection = DriverManager.getConnection(SOURCE_URL, createProperties());
        targetConnection = DriverManager.getConnection(TARGET_URL, createProperties());

        cleanupSlot();

        // Ensure target table exists and is empty
        try (Statement stmt = targetConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
            stmt.execute("""
                CREATE TABLE customers_100k_benchmark (
                    customer_id INTEGER,
                    name VARCHAR(100),
                    email VARCHAR(150),
                    phone VARCHAR(20),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """);
        }

        // Ensure source has at least 100,000 rows
        long sourceCount = getRowCount(sourceConnection, "customers");
        if (sourceCount < 100_000) {
            long rowsToInsert = 100_000 - sourceCount;
            System.out.println("[BENCHMARK] Inserting " + rowsToInsert + " additional rows into source customers...");
            try (PreparedStatement ps = sourceConnection.prepareStatement(
                    "INSERT INTO customers (name, email, phone) " +
                    "SELECT 'Bench Customer ' || i, 'bench_' || i || '@test.com', '9999999999' " +
                    "FROM generate_series(?, ?) AS i")) {
                ps.setLong(1, sourceCount + 1);
                ps.setLong(2, 100_000);
                ps.executeUpdate();
            }
        }

        snapshotManager = new SnapshotManager(SOURCE_URL, USER, PASSWORD);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (snapshotManager != null) {
            snapshotManager.close();
        }
        cleanupSlot();

        if (targetConnection != null && !targetConnection.isClosed()) {
            try (Statement stmt = targetConnection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
                stmt.execute("DELETE FROM table_state");
                stmt.execute("UPDATE bootstrap_state SET status = 'NEW', snapshot_lsn = NULL, error_message = NULL WHERE id = 1");
            }
            targetConnection.close();
        }
        if (sourceConnection != null && !sourceConnection.isClosed()) {
            sourceConnection.close();
        }
    }

    private void cleanupSlot() {
        try (PreparedStatement ps = sourceConnection.prepareStatement(
                "SELECT pg_drop_replication_slot(?) FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, BENCHMARK_SLOT);
            ps.setString(2, BENCHMARK_SLOT);
            ps.execute();
        } catch (SQLException ignored) {}
    }

    private long getRowCount(Connection conn, String table) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void testSyncWith100kRows() throws Exception {
        long totalSourceRows = getRowCount(sourceConnection, "customers");
        System.out.println("=================================================");
        System.out.println("🚀 STARTING 100K+ ROWS SYNC BENCHMARK");
        System.out.println("Source Customers Total Rows: " + totalSourceRows);
        System.out.println("=================================================");

        assertTrue(totalSourceRows >= 100_000, "Source must have at least 100,000 rows");

        // 1. Create exported snapshot slot
        SnapshotManager.SnapshotInfo info = snapshotManager.createExportedSnapshot(BENCHMARK_SLOT);
        String snapshotId = info.snapshotName();
        String snapshotLsn = info.consistentPoint();

        System.out.println("[BENCHMARK] Exported snapshot created. ID: " + snapshotId + ", LSN: " + snapshotLsn);

        // 2. Initialize StateManager on Target and register table mapping
        SnapshotStateManager stateManager = new SnapshotStateManager(targetConnection);
        stateManager.initialize();
        stateManager.beginBootstrap(snapshotLsn, java.util.Map.of("customers", TARGET_TABLE));

        SnapshotWorker worker = new SnapshotWorker(
                SOURCE_URL, TARGET_URL, USER, PASSWORD, stateManager
        );

        // 3. Measure bulk copy performance
        long startTime = System.currentTimeMillis();

        long copiedRows = worker.copyTable(snapshotId, "customers", TARGET_TABLE);

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        double durationSec = durationMs / 1000.0;
        double throughput = copiedRows / (durationSec > 0 ? durationSec : 0.001);

        System.out.println("=================================================");
        System.out.println("✅ 100K+ ROWS SYNC COMPLETED SUCCESSFULLY");
        System.out.println("Rows Copied: " + copiedRows);
        System.out.println("Time Elapsed: " + String.format("%.2f", durationSec) + " seconds (" + durationMs + " ms)");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " rows/second");
        System.out.println("Batch Size: 500 rows/batch");
        System.out.println("=================================================");

        // 4. Assertions
        assertEquals(totalSourceRows, copiedRows, "Copied rows must match total source rows");
        long targetCount = getRowCount(targetConnection, TARGET_TABLE);
        assertEquals(totalSourceRows, targetCount, "Target table row count must equal source table row count");
        assertTrue(copiedRows >= 100_000, "Verified >= 100,000 rows synchronized");
    }
}
