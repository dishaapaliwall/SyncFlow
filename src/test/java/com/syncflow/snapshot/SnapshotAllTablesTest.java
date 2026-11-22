package com.syncflow.snapshot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

public class SnapshotAllTablesTest {

    private static final String SOURCE_URL =
            "jdbc:postgresql://localhost:5434/syncflow_source";

    private static final String TARGET_URL =
            "jdbc:postgresql://localhost:5435/syncflow_target";

    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String SLOT =
            "snapshot_all_tables_test_slot";

    private static final Map<String, String> TABLES =
            new LinkedHashMap<>();

    static {
        TABLES.put("customers", "snapshot_customers_test");
        TABLES.put("products", "snapshot_products_test");
        TABLES.put("orders", "snapshot_orders_test");
        TABLES.put("order_items", "snapshot_order_items_test");
        TABLES.put("payments", "snapshot_payments_test");
    }

    @Test
    void testAllTablesSnapshot() throws Exception {
        main(new String[0]);
    }

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));

        SnapshotManager snapshotManager = null;
        SnapshotStateManager stateManager = null;
        boolean slotCreated = false;

        try {
            // 1. Prepare separate scratch tables on the target.
            createScratchTables();

            // 2. Initialize bootstrap metadata.
            stateManager = new SnapshotStateManager(
                    DriverManager.getConnection(
                            TARGET_URL, USER, PASSWORD
                    )
            );

            stateManager.initialize();

            // 3. Create one exported snapshot.
            snapshotManager = new SnapshotManager(
                    SOURCE_URL, USER, PASSWORD
            );

            SnapshotManager.SnapshotInfo snapshot =
                    snapshotManager.createExportedSnapshot(SLOT);

            slotCreated = true;

            System.out.println("\n===== EXPORTED SNAPSHOT =====");
            System.out.println("Consistent LSN: " +
                    snapshot.consistentPoint());
            System.out.println("Snapshot name: " +
                    snapshot.snapshotName());

            // 4. Register all tables in bootstrap metadata.
            stateManager.beginBootstrap(
                    snapshot.consistentPoint(),
                    List.copyOf(TABLES.keySet())
            );

            // 5. Read expected counts from the exported snapshot.
            Map<String, Long> expectedCounts =
                    getSnapshotCounts(snapshot.snapshotName());

            // 6. Copy all five tables using the SAME snapshot.
            SnapshotWorker worker = new SnapshotWorker(
                    SOURCE_URL,
                    TARGET_URL,
                    USER,
                    PASSWORD,
                    stateManager
            );

            Map<String, Long> copiedCounts = new LinkedHashMap<>();

            System.out.println("\n===== COPYING TABLES =====");

            for (Map.Entry<String, String> entry : TABLES.entrySet()) {

                String sourceTable = entry.getKey();
                String targetTable = entry.getValue();

                long copied = worker.copyTable(
                        snapshot.snapshotName(),
                        sourceTable,
                        targetTable
                );

                copiedCounts.put(sourceTable, copied);
            }

            // 7. Verify target scratch-table counts.
            System.out.println("\n===== VERIFICATION =====");

            boolean allMatched = true;

            for (Map.Entry<String, String> entry : TABLES.entrySet()) {

                String sourceTable = entry.getKey();
                String scratchTable = entry.getValue();

                long expected = expectedCounts.get(sourceTable);
                long actual = getTargetCount(scratchTable);

                System.out.println(
                        sourceTable +
                                " | Snapshot: " + expected +
                                " | Copied: " + copiedCounts.get(sourceTable) +
                                " | Target: " + actual
                );

                if (expected != actual ||
                        expected != copiedCounts.get(sourceTable)) {
                    allMatched = false;
                }
            }

            System.out.println("\nAll tables matched: " + allMatched);

            if (!allMatched) {
                throw new RuntimeException(
                        "Snapshot row-count verification failed."
                );
            }

            System.out.println(
                    "\nALL FIVE TABLES COPIED SUCCESSFULLY."
            );

        } finally {

            // Release exporter only after all workers have imported
            // and finished using the exported snapshot.
            if (snapshotManager != null) {
                snapshotManager.release();
            }

            if (slotCreated) {
                dropTestSlot();
            }

            dropScratchTables();

            resetTestMetadata();


        }
    }

    private static void createScratchTables() throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
                     );
             Statement statement = connection.createStatement()) {

            for (Map.Entry<String, String> entry : TABLES.entrySet()) {

                String scratchTable = entry.getValue();
                String sourceTable = entry.getKey();

                statement.execute(
                        "DROP TABLE IF EXISTS public.\"" +
                                scratchTable + "\""
                );

                statement.execute(
                        "CREATE TABLE public.\"" +
                                scratchTable +
                                "\" (LIKE public.\"" +
                                sourceTable +
                                "\" INCLUDING ALL)"
                );
            }
        }

        System.out.println("[TEST] Scratch tables created.");
    }

    private static Map<String, Long> getSnapshotCounts(
            String snapshotName
    ) throws Exception {

        Map<String, Long> counts = new LinkedHashMap<>();

        try (Connection connection =
                     DriverManager.getConnection(
                             SOURCE_URL, USER, PASSWORD
                     )) {

            connection.setTransactionIsolation(
                    Connection.TRANSACTION_REPEATABLE_READ
            );
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {

                statement.execute(
                        "SET TRANSACTION SNAPSHOT '" +
                                snapshotName + "'"
                );

                for (String table : TABLES.keySet()) {

                    try (ResultSet rs = statement.executeQuery(
                            "SELECT COUNT(*) FROM public.\"" +
                                    table + "\""
                    )) {
                        rs.next();
                        counts.put(table, rs.getLong(1));
                    }
                }
            }

            connection.rollback();
        }

        return counts;
    }

    private static long getTargetCount(
            String table
    ) throws Exception {

        try (Connection connection =
                     DriverManager.getConnection(
                             TARGET_URL, USER, PASSWORD
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

            statement.execute(
                    "SELECT pg_drop_replication_slot('" + SLOT + "')"
            );
        }

        System.out.println("[CLEANUP] Test replication slot dropped.");
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

        System.out.println("[CLEANUP] Scratch tables dropped.");
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

        System.out.println("[CLEANUP] Bootstrap metadata reset to NEW.");
    }
}