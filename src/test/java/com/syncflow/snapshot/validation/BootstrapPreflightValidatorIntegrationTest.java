package com.syncflow.snapshot.validation;

import com.syncflow.service.SyncService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class BootstrapPreflightValidatorIntegrationTest {

    private static final String SOURCE_URL = "jdbc:postgresql://localhost:5434/syncflow_source";
    private static final String TARGET_URL = "jdbc:postgresql://localhost:5435/syncflow_target";
    private static final String USER = "syncflow";
    private static final String PASSWORD = "syncflow123";

    private static final String SRC_TABLE = "preflight_src_test";
    private static final String TGT_TABLE = "preflight_tgt_test";
    private static final String TEST_SLOT = "preflight_test_slot";
    private static final String TEST_PUB = "preflight_test_pub";

    private BootstrapPreflightValidator validator;

    @BeforeAll
    static void initTimeZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @BeforeEach
    void setUp() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        validator = new BootstrapPreflightValidator(
                SOURCE_URL, USER, PASSWORD,
                TARGET_URL, USER, PASSWORD);
        cleanupResources();
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanupResources();
    }

    private Connection getConnection(String url) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("options", "-c TimeZone=UTC");
        return DriverManager.getConnection(url, props);
    }

    private void cleanupResources() {
        // Drop slot if exists
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_drop_replication_slot('" + TEST_SLOT + "')");
        } catch (Exception ignored) {
        }

        // Drop publication if exists
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP PUBLICATION IF EXISTS " + TEST_PUB);
        } catch (Exception ignored) {
        }

        // Drop scratch tables
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS public.\"" + SRC_TABLE + "\" CASCADE");
        } catch (Exception ignored) {
        }

        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS public.\"" + TGT_TABLE + "\" CASCADE");
        } catch (Exception ignored) {
        }

        // Reset bootstrap metadata to NEW for clean preflight testing
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
        } catch (Exception ignored) {
        }
    }

    private void createCompatibleTables() throws SQLException {
        try (Connection srcConn = getConnection(SOURCE_URL);
                Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_src_test" (
                        id INT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
                Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_tgt_test" (
                        id INT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }
    }

    private void createPublicationForTable(String table) throws SQLException {
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PUBLICATION " + TEST_PUB + " FOR TABLE public.\"" + table + "\"");
        }
    }

    @Test
    void testSuccessfulValidation() throws Exception {
        createCompatibleTables();
        createPublicationForTable(SRC_TABLE);

        PreflightReport report = validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE));

        assertTrue(report.isSuccess());
        assertTrue(report.getErrors().isEmpty());
        assertTrue(report.getChecks().stream().anyMatch(c -> c.checkName().equals("WAL_LEVEL") && c.passed()));
        assertTrue(report.getChecks().stream().anyMatch(c -> c.checkName().equals("SLOT_CAPACITY") && c.passed()));
        assertTrue(report.getChecks().stream()
                .anyMatch(c -> c.checkName().equals("PUBLICATION_MEMBERSHIP") && c.passed()));
        assertTrue(report.getChecks().stream()
                .anyMatch(c -> c.checkName().startsWith("TARGET_TABLE_EMPTY_") && c.passed()));
    }

    @Test
    void testFailsWhenTargetTableNotEmpty() throws Exception {
        createCompatibleTables();
        createPublicationForTable(SRC_TABLE);

        // Insert row into target table
        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO public.\"" + TGT_TABLE + "\" (id, name) VALUES (1, 'existing_data')");
        }

        PreflightValidationException ex = assertThrows(
                PreflightValidationException.class,
                () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE)));

        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("is not empty")));
    }

    @Test
    void testFailsWhenPublicationMissingTable() throws Exception {
        createCompatibleTables();
        // Create publication for another dummy table or empty
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE public.preflight_other_table (id INT PRIMARY KEY)");
            stmt.execute("CREATE PUBLICATION " + TEST_PUB + " FOR TABLE public.preflight_other_table");
        }

        try {
            PreflightValidationException ex = assertThrows(
                    PreflightValidationException.class,
                    () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE)));

            assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("does not include requested source table(s)")));
        } finally {
            try (Connection conn = getConnection(SOURCE_URL);
                    Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS public.preflight_other_table CASCADE");
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void testFailsWhenSourceTableDoesNotExist() throws Exception {
        createCompatibleTables();
        createPublicationForTable(SRC_TABLE);

        PreflightValidationException ex = assertThrows(
                PreflightValidationException.class,
                () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of("non_existent_table", TGT_TABLE)));

        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("does not exist in source database")));
    }

    @Test
    void testFailsWhenSourceTableHasNoPrimaryKey() throws Exception {
        // Create source table without primary key and default replica identity
        try (Connection srcConn = getConnection(SOURCE_URL);
                Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_src_test" (
                        id INT,
                        name VARCHAR(100)
                    )
                    """);
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
                Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_tgt_test" (
                        id INT,
                        name VARCHAR(100)
                    )
                    """);
        }

        createPublicationForTable(SRC_TABLE);

        PreflightValidationException ex = assertThrows(
                PreflightValidationException.class,
                () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE)));

        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("DEFAULT replica identity but no primary key")));
    }

    @Test
    void testPassesWhenReplicaIdentityIsFull() throws Exception {
        // Table without primary key, but with REPLICA IDENTITY FULL
        try (Connection srcConn = getConnection(SOURCE_URL);
                Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_src_test" (
                        id INT,
                        name VARCHAR(100)
                    )
                    """);
            stmt.execute("ALTER TABLE public.\"preflight_src_test\" REPLICA IDENTITY FULL");
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
                Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_tgt_test" (
                        id INT,
                        name VARCHAR(100)
                    )
                    """);
        }

        createPublicationForTable(SRC_TABLE);

        PreflightReport report = validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE));
        assertTrue(report.isSuccess());
        assertTrue(report.getChecks().stream()
                .anyMatch(c -> c.checkName().equals("REPLICA_IDENTITY_" + SRC_TABLE) && c.passed()));
    }

    @Test
    void testPassesWhenReplicaIdentityIsIndex() throws Exception {
        // Table with unique index used as REPLICA IDENTITY
        try (Connection srcConn = getConnection(SOURCE_URL);
                Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_src_test" (
                        id INT NOT NULL,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
            stmt.execute("CREATE UNIQUE INDEX preflight_src_idx ON public.\"preflight_src_test\" (id)");
            stmt.execute("ALTER TABLE public.\"preflight_src_test\" REPLICA IDENTITY USING INDEX preflight_src_idx");
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
                Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_tgt_test" (
                        id INT NOT NULL,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
        }

        createPublicationForTable(SRC_TABLE);

        PreflightReport report = validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE));
        assertTrue(report.isSuccess());
        assertTrue(report.getChecks().stream()
                .anyMatch(c -> c.checkName().equals("REPLICA_IDENTITY_" + SRC_TABLE) && c.passed()));
    }

    @Test
    void testPassesWhenPublicationIsForAllTables() throws Exception {
        createCompatibleTables();
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PUBLICATION " + TEST_PUB + " FOR ALL TABLES");
        }

        PreflightReport report = validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE));
        assertTrue(report.isSuccess());
        assertTrue(report.getChecks().stream().anyMatch(c -> c.details().contains("FOR ALL TABLES")));
    }

    @Test
    void testFailsWhenTargetHasGeneratedColumnAlways() throws Exception {
        try (Connection srcConn = getConnection(SOURCE_URL);
                Statement stmt = srcConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_src_test" (
                        id INT PRIMARY KEY,
                        computed INT
                    )
                    """);
        }

        try (Connection tgtConn = getConnection(TARGET_URL);
                Statement stmt = tgtConn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE public."preflight_tgt_test" (
                        id INT PRIMARY KEY,
                        computed INT GENERATED ALWAYS AS (id * 2) STORED
                    )
                    """);
        }

        createPublicationForTable(SRC_TABLE);

        PreflightValidationException ex = assertThrows(
                PreflightValidationException.class,
                () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE)));

        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("generated column (ALWAYS AS)")));
    }

    @Test
    void testFailsWhenSlotAlreadyExists() throws Exception {
        createCompatibleTables();
        createPublicationForTable(SRC_TABLE);

        // Pre-create the replication slot
        try (Connection conn = getConnection(SOURCE_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_create_logical_replication_slot('" + TEST_SLOT + "', 'pgoutput')");
        }

        PreflightValidationException ex = assertThrows(
                PreflightValidationException.class,
                () -> validator.validate(TEST_SLOT, TEST_PUB, Map.of(SRC_TABLE, TGT_TABLE)));

        assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("already exists on source database")));
    }

    @Test
    void testSyncServiceIntegrationFailsFastBeforeCreatingSlot() throws Exception {
        createCompatibleTables();
        createPublicationForTable(SRC_TABLE);

        // Put row in target table so preflight will fail
        try (Connection conn = getConnection(TARGET_URL);
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO public.\"" + TGT_TABLE + "\" (id, name) VALUES (999, 'dirty_target')");
        }

        try (SyncService service = new SyncService(
                SOURCE_URL, USER, PASSWORD,
                TEST_SLOT, TEST_PUB,
                TARGET_URL, USER, PASSWORD)) {
            PreflightValidationException ex = assertThrows(
                    PreflightValidationException.class,
                    () -> service.bootstrapAndStart(Map.of(SRC_TABLE, TGT_TABLE)));
            assertTrue(ex.getErrors().stream().anyMatch(e -> e.contains("is not empty")));

            // Verify that the replication slot was NEVER created on source
            try (Connection conn = getConnection(SOURCE_URL);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + TEST_SLOT + "'")) {
                assertFalse(rs.next(), "Replication slot should NOT have been created when preflight fails!");
            }
        }
    }
}
