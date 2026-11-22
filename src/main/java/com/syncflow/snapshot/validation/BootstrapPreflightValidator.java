package com.syncflow.snapshot.validation;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class BootstrapPreflightValidator {

    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private final String sourceUrl;
    private final String sourceUsername;
    private final String sourcePassword;

    private final String targetUrl;
    private final String targetUsername;
    private final String targetPassword;

    public BootstrapPreflightValidator(
            String sourceUrl,
            String sourceUsername,
            String sourcePassword,
            String targetUrl,
            String targetUsername,
            String targetPassword
    ) {
        this.sourceUrl = sourceUrl;
        this.sourceUsername = sourceUsername;
        this.sourcePassword = sourcePassword;
        this.targetUrl = targetUrl;
        this.targetUsername = targetUsername;
        this.targetPassword = targetPassword;
    }

    private Properties createProperties(String username, String password) {
        // Fix Windows default "Asia/Calcutta" issue for PostgreSQL JDBC
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("options", "-c TimeZone=UTC");
        return props;
    }

    /**
     * Executes read-only preflight validations against source and target databases.
     * Never creates slots, exported snapshots, or alters tables/data.
     */
    public PreflightReport validate(
            String slotName,
            String publicationName,
            Map<String, String> tables
    ) throws PreflightValidationException, SQLException {

        PreflightReport report = new PreflightReport();

        // 8. Validate table mapping & identifiers upfront
        validateTableMapping(tables, report);
        if (!report.isSuccess()) {
            throw new PreflightValidationException(
                    "Bootstrap preflight validation failed on table mapping.",
                    report.getErrors(),
                    report
            );
        }

        try (Connection sourceConn = DriverManager.getConnection(sourceUrl, createProperties(sourceUsername, sourcePassword));
             Connection targetConn = DriverManager.getConnection(targetUrl, createProperties(targetUsername, targetPassword))) {

            validateConnection(sourceConn, targetConn, slotName, publicationName, tables, report);

        } catch (SQLException e) {
            report.addError("DATABASE_CONNECTIVITY", "Database connection error during preflight: " + e.getMessage());
            throw new PreflightValidationException(
                    "Database connectivity failed during preflight validation: " + e.getMessage(),
                    report.getErrors(),
                    report
            );
        }

        if (!report.isSuccess()) {
            throw new PreflightValidationException(
                    "Bootstrap preflight validation failed with " + report.getErrors().size() + " error(s).",
                    report.getErrors(),
                    report
            );
        }

        return report;
    }

    /**
     * Overloaded method allowing execution with pre-established connections (useful for testing/mocking).
     */
    public void validateConnection(
            Connection sourceConn,
            Connection targetConn,
            String slotName,
            String publicationName,
            Map<String, String> tables,
            PreflightReport report
    ) throws SQLException {

        // 1. Check wal_level supports logical replication
        validateWalLevel(sourceConn, report);

        // 2. Available logical replication slot capacity and duplicate check
        validateSlotCapacity(sourceConn, slotName, report);

        // 3. Publication exists and includes all requested source tables
        validatePublication(sourceConn, publicationName, tables.keySet(), report);

        // 4. Requested source tables exist
        validateSourceTablesExist(sourceConn, tables.keySet(), report);

        // 5. Source tables have suitable replica identity/primary-key configuration
        validateReplicaIdentity(sourceConn, tables.keySet(), report);

        // 6 & 7. Schema compatibility and target table emptiness
        validateTargetSchemasAndEmptiness(sourceConn, targetConn, tables, report);

        // 9. WAL retention configuration and relevant risks
        validateWalRetentionRisks(sourceConn, report);
    }

    // --- Validation 1: Source wal_level ---
    public void validateWalLevel(Connection sourceConn, PreflightReport report) throws SQLException {
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            if (rs.next()) {
                String walLevel = rs.getString(1);
                if ("logical".equalsIgnoreCase(walLevel)) {
                    report.addCheck("WAL_LEVEL", true, "wal_level is 'logical'.");
                } else {
                    report.addError(
                            "WAL_LEVEL",
                            "Source PostgreSQL wal_level is '" + walLevel + "', but must be 'logical' to support logical replication."
                    );
                }
            } else {
                report.addError("WAL_LEVEL", "Unable to determine source PostgreSQL wal_level.");
            }
        }
    }

    // --- Validation 2: Replication Slot Capacity & Existing Slot ---
    public void validateSlotCapacity(Connection sourceConn, String slotName, PreflightReport report) throws SQLException {
        if (slotName == null || !VALID_IDENTIFIER.matcher(slotName).matches()) {
            report.addError("SLOT_NAME", "Replication slot name is invalid: " + slotName);
            return;
        }

        int maxSlots = 0;
        int activeSlots = 0;
        boolean slotAlreadyExists = false;

        try (Statement stmt = sourceConn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT current_setting('max_replication_slots')::int")) {
                if (rs.next()) {
                    maxSlots = rs.getInt(1);
                }
            }

            try (PreparedStatement ps = sourceConn.prepareStatement(
                    "SELECT slot_name FROM pg_replication_slots")) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        activeSlots++;
                        if (slotName.equals(rs.getString("slot_name"))) {
                            slotAlreadyExists = true;
                        }
                    }
                }
            }
        }

        if (slotAlreadyExists) {
            report.addError(
                    "SLOT_EXISTS",
                    "Replication slot '" + slotName + "' already exists on source database. Initial bootstrap requires a fresh slot."
            );
            return;
        }

        if (activeSlots >= maxSlots) {
            report.addError(
                    "SLOT_CAPACITY",
                    "Insufficient replication slot capacity on source database: " + activeSlots
                            + " of " + maxSlots + " slots are in use. Increase max_replication_slots or drop unused slots."
            );
        } else {
            report.addCheck(
                    "SLOT_CAPACITY",
                    true,
                    "Replication slot capacity available: " + activeSlots + " of " + maxSlots + " in use. Slot '" + slotName + "' does not exist."
            );
        }
    }

    // --- Validation 3: Publication & Table Membership ---
    public void validatePublication(
            Connection sourceConn,
            String publicationName,
            Set<String> sourceTables,
            PreflightReport report
    ) throws SQLException {

        if (publicationName == null || !VALID_IDENTIFIER.matcher(publicationName).matches()) {
            report.addError("PUBLICATION", "Invalid publication name: " + publicationName);
            return;
        }

        boolean pubExists = false;
        boolean pubAllTables = false;

        try (PreparedStatement ps = sourceConn.prepareStatement(
                "SELECT puballtables FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    pubExists = true;
                    pubAllTables = rs.getBoolean("puballtables");
                }
            }
        }

        if (!pubExists) {
            report.addError("PUBLICATION", "Publication '" + publicationName + "' does not exist on source database.");
            return;
        }

        if (pubAllTables) {
            report.addCheck("PUBLICATION", true, "Publication '" + publicationName + "' is configured FOR ALL TABLES.");
            return;
        }

        // Query tables included in the publication
        Set<String> publishedTables = new HashSet<>();
        try (PreparedStatement ps = sourceConn.prepareStatement(
                "SELECT tablename FROM pg_publication_tables WHERE pubname = ? AND schemaname = 'public'")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    publishedTables.add(rs.getString("tablename"));
                }
            }
        }

        List<String> missingFromPub = new ArrayList<>();
        for (String table : sourceTables) {
            if (!publishedTables.contains(table)) {
                missingFromPub.add(table);
            }
        }

        if (!missingFromPub.isEmpty()) {
            report.addError(
                    "PUBLICATION_MEMBERSHIP",
                    "Publication '" + publicationName + "' does not include requested source table(s): " + missingFromPub
            );
        } else {
            report.addCheck(
                    "PUBLICATION_MEMBERSHIP",
                    true,
                    "All requested tables are included in publication '" + publicationName + "'."
            );
        }
    }

    // --- Validation 4: Source Tables Exist ---
    public void validateSourceTablesExist(
            Connection sourceConn,
            Set<String> sourceTables,
            PreflightReport report
    ) throws SQLException {

        for (String table : sourceTables) {
            try (PreparedStatement ps = sourceConn.prepareStatement(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        report.addError("SOURCE_TABLE_EXISTS", "Requested source table '" + table + "' does not exist in source database.");
                    }
                }
            }
        }

        if (report.getErrors().stream().noneMatch(e -> e.contains("SOURCE_TABLE_EXISTS"))) {
            report.addCheck("SOURCE_TABLE_EXISTS", true, "All requested source tables exist.");
        }
    }

    // --- Validation 5: Replica Identity & Primary Key Configuration ---
    public void validateReplicaIdentity(
            Connection sourceConn,
            Set<String> sourceTables,
            PreflightReport report
    ) throws SQLException {

        for (String table : sourceTables) {
            try (PreparedStatement ps = sourceConn.prepareStatement("""
                    SELECT c.relreplident,
                           (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) AS pk_count,
                           (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident) AS replident_index_count
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relname = ?
                    """)) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        // Handled in source tables check
                        continue;
                    }

                    String replident = rs.getString("relreplident");
                    int pkCount = rs.getInt("pk_count");
                    int replidentIndexCount = rs.getInt("replident_index_count");

                    if ("d".equals(replident)) {
                        // DEFAULT: requires a primary key
                        if (pkCount > 0) {
                            report.addCheck("REPLICA_IDENTITY_" + table, true, "Table '" + table + "' has DEFAULT replica identity with a valid primary key.");
                        } else {
                            report.addError(
                                    "REPLICA_IDENTITY_" + table,
                                    "Source table '" + table + "' has DEFAULT replica identity but no primary key. "
                                            + "Logical replication requires a primary key or REPLICA IDENTITY FULL to safely replicate UPDATE and DELETE operations."
                            );
                        }
                    } else if ("f".equals(replident)) {
                        // FULL replica identity: valid for UPDATE and DELETE
                        report.addCheck("REPLICA_IDENTITY_" + table, true, "Table '" + table + "' is configured with REPLICA IDENTITY FULL.");
                    } else if ("i".equals(replident)) {
                        // INDEX replica identity: valid if index exists
                        if (replidentIndexCount > 0) {
                            report.addCheck("REPLICA_IDENTITY_" + table, true, "Table '" + table + "' is configured with REPLICA IDENTITY INDEX.");
                        } else {
                            report.addError(
                                    "REPLICA_IDENTITY_" + table,
                                    "Source table '" + table + "' has REPLICA IDENTITY INDEX but no active replica identity index."
                            );
                        }
                    } else {
                        // NOTHING ('n')
                        report.addError(
                                "REPLICA_IDENTITY_" + table,
                                "Source table '" + table + "' has REPLICA IDENTITY NOTHING. "
                                        + "UPDATE and DELETE operations cannot be replicated."
                        );
                    }
                }
            }
        }
    }

    // --- Validation 6 & 7: Schema Compatibility and Target Table Emptiness ---
    public void validateTargetSchemasAndEmptiness(
            Connection sourceConn,
            Connection targetConn,
            Map<String, String> tables,
            PreflightReport report
    ) throws SQLException {

        for (Map.Entry<String, String> entry : tables.entrySet()) {
            String sourceTable = entry.getKey();
            String targetTable = entry.getValue();

            // Check target table existence
            try (PreparedStatement ps = targetConn.prepareStatement(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?")) {
                ps.setString(1, targetTable);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        report.addError("TARGET_TABLE_EXISTS", "Target table '" + targetTable + "' does not exist in target database.");
                        continue;
                    }
                }
            }

            // Check target table emptiness
            validateTargetTableEmpty(targetConn, targetTable, report);

            // Compare column schemas between source and target
            compareColumnSchemas(sourceConn, targetConn, sourceTable, targetTable, report);
        }
    }

    private void validateTargetTableEmpty(
            Connection targetConn,
            String targetTable,
            PreflightReport report
    ) throws SQLException {
        // Safe identifier quoting after validation
        String sql = "SELECT COUNT(*) FROM public.\"" + targetTable + "\"";
        try (Statement stmt = targetConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long count = rs.getLong(1);
                if (count > 0) {
                    report.addError(
                            "TARGET_TABLE_EMPTY_" + targetTable,
                            "Target table '" + targetTable + "' is not empty (contains " + count + " rows). "
                                    + "Initial snapshot requires an empty target table to avoid duplicate data or primary key collisions."
                    );
                } else {
                    report.addCheck("TARGET_TABLE_EMPTY_" + targetTable, true, "Target table '" + targetTable + "' is empty.");
                }
            }
        }
    }

    private record ColumnInfo(
            String name,
            String udtName,
            Integer maxLength,
            Integer numericPrecision,
            Integer numericScale,
            String isNullable,
            String isGenerated,
            String identityGeneration
    ) {}

    private void compareColumnSchemas(
            Connection sourceConn,
            Connection targetConn,
            String sourceTable,
            String targetTable,
            PreflightReport report
    ) throws SQLException {

        Map<String, ColumnInfo> sourceCols = loadColumns(sourceConn, sourceTable);
        Map<String, ColumnInfo> targetCols = loadColumns(targetConn, targetTable);

        for (Map.Entry<String, ColumnInfo> entry : sourceCols.entrySet()) {
            String colName = entry.getKey();
            ColumnInfo srcCol = entry.getValue();
            ColumnInfo tgtCol = targetCols.get(colName);

            if (tgtCol == null) {
                report.addError(
                        "SCHEMA_COMPATIBILITY_" + targetTable,
                        "Target table '" + targetTable + "' is missing column '" + colName + "' present in source table '" + sourceTable + "'."
                );
                continue;
            }

            // Check generated column incompatibility:
            // SnapshotWorker attempts to insert into all columns selected from source.
            // If target column is generated ALWAYS, PostgreSQL prohibits INSERT into it.
            if ("ALWAYS".equalsIgnoreCase(tgtCol.isGenerated())) {
                report.addError(
                        "SCHEMA_GENERATED_COLUMN_" + targetTable,
                        "Target column '" + targetTable + "." + colName + "' is a generated column (ALWAYS AS). Snapshot copying cannot insert into generated columns."
                );
            }

            // Check identity column incompatibility:
            // If target column has GENERATED ALWAYS AS IDENTITY, INSERT without OVERRIDING SYSTEM VALUE will fail.
            if ("ALWAYS".equalsIgnoreCase(tgtCol.identityGeneration())) {
                report.addError(
                        "SCHEMA_IDENTITY_COLUMN_" + targetTable,
                        "Target column '" + targetTable + "." + colName + "' has GENERATED ALWAYS AS IDENTITY. Snapshot copying cannot insert into GENERATED ALWAYS columns without identity override."
                );
            }

            // Type compatibility
            if (!isTypeCompatible(srcCol.udtName(), tgtCol.udtName())) {
                report.addError(
                        "SCHEMA_TYPE_MISMATCH_" + targetTable,
                        "Column '" + colName + "' type mismatch: source table '" + sourceTable + "' has type '" + srcCol.udtName()
                                + "', but target table '" + targetTable + "' has incompatible type '" + tgtCol.udtName() + "'."
                );
            }

            // Character length truncation risk:
            // If source max length > target max length, data could be truncated or fail.
            if (srcCol.maxLength() != null && tgtCol.maxLength() != null) {
                if (srcCol.maxLength() > tgtCol.maxLength()) {
                    report.addError(
                            "SCHEMA_LENGTH_MISMATCH_" + targetTable,
                            "Column '" + colName + "' length mismatch: source has max length " + srcCol.maxLength()
                                    + ", but target has smaller max length " + tgtCol.maxLength() + "."
                    );
                }
            }
        }

        if (report.getErrors().stream().noneMatch(e -> e.contains("_" + targetTable))) {
            report.addCheck("SCHEMA_COMPATIBILITY_" + targetTable, true, "Schema of '" + targetTable + "' is compatible with source '" + sourceTable + "'.");
        }
    }

    private Map<String, ColumnInfo> loadColumns(Connection conn, String tableName) throws SQLException {
        Map<String, ColumnInfo> columns = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT column_name, udt_name, character_maximum_length,
                       numeric_precision, numeric_scale, is_nullable,
                       is_generated, identity_generation
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                ORDER BY ordinal_position
                """)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("column_name");
                    String udt = rs.getString("udt_name");
                    Integer maxLen = rs.getObject("character_maximum_length") != null ? rs.getInt("character_maximum_length") : null;
                    Integer numPrec = rs.getObject("numeric_precision") != null ? rs.getInt("numeric_precision") : null;
                    Integer numScale = rs.getObject("numeric_scale") != null ? rs.getInt("numeric_scale") : null;
                    String isNull = rs.getString("is_nullable");
                    String isGen = rs.getString("is_generated");
                    String idGen = rs.getString("identity_generation");

                    columns.put(name, new ColumnInfo(name, udt, maxLen, numPrec, numScale, isNull, isGen, idGen));
                }
            }
        }
        return columns;
    }

    private boolean isTypeCompatible(String srcUdt, String tgtUdt) {
        if (srcUdt == null || tgtUdt == null) return false;
        if (srcUdt.equalsIgnoreCase(tgtUdt)) return true;

        // Compatible widening / equivalent types
        Set<String> stringTypes = Set.of("varchar", "text", "bpchar");
        if (stringTypes.contains(srcUdt.toLowerCase()) && stringTypes.contains(tgtUdt.toLowerCase())) {
            return true;
        }

        // Integer widening
        if ("int2".equalsIgnoreCase(srcUdt) && ("int4".equalsIgnoreCase(tgtUdt) || "int8".equalsIgnoreCase(tgtUdt))) {
            return true;
        }
        if ("int4".equalsIgnoreCase(srcUdt) && "int8".equalsIgnoreCase(tgtUdt)) {
            return true;
        }

        // Float widening
        if ("float4".equalsIgnoreCase(srcUdt) && "float8".equalsIgnoreCase(tgtUdt)) {
            return true;
        }

        // Numeric equivalence
        if ("numeric".equalsIgnoreCase(srcUdt) && "numeric".equalsIgnoreCase(tgtUdt)) {
            return true;
        }

        return false;
    }

    // --- Validation 8: Table Mapping & Identifier Validation ---
    public void validateTableMapping(Map<String, String> tables, PreflightReport report) {
        if (tables == null || tables.isEmpty()) {
            report.addError("TABLE_MAPPING", "Bootstrap table mapping cannot be null or empty.");
            return;
        }

        Set<String> targetNames = new HashSet<>();
        for (Map.Entry<String, String> entry : tables.entrySet()) {
            String sourceTable = entry.getKey();
            String targetTable = entry.getValue();

            if (sourceTable == null || !VALID_IDENTIFIER.matcher(sourceTable).matches()) {
                report.addError("TABLE_IDENTIFIER", "Source table name is not a valid SQL identifier: " + sourceTable);
            }

            if (targetTable == null || !VALID_IDENTIFIER.matcher(targetTable).matches()) {
                report.addError("TABLE_IDENTIFIER", "Target table name is not a valid SQL identifier: " + targetTable);
            } else if (!targetNames.add(targetTable)) {
                report.addError("DUPLICATE_TARGET_TABLE", "Multiple source tables map to the same target table: " + targetTable);
            }
        }

        if (report.getErrors().stream().noneMatch(e -> e.contains("TABLE_"))) {
            report.addCheck("TABLE_MAPPING", true, "Table mapping validated for " + tables.size() + " table(s).");
        }
    }

    // --- Validation 9: WAL Retention Risks ---
    public void validateWalRetentionRisks(Connection sourceConn, PreflightReport report) throws SQLException {
        try (Statement stmt = sourceConn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SHOW max_slot_wal_keep_size")) {
                if (rs.next()) {
                    String val = rs.getString(1);
                    if ("-1".equals(val) || "-1MB".equalsIgnoreCase(val)) {
                        report.addWarning(
                                "max_slot_wal_keep_size is set to -1 (unbounded). "
                                        + "Risk: If CDC stops or lags behind, source PostgreSQL WAL will accumulate indefinitely on disk, risking storage exhaustion."
                        );
                    } else {
                        report.addCheck(
                                "WAL_RETENTION_SETTING",
                                true,
                                "max_slot_wal_keep_size is configured to '" + val + "'. "
                                        + "Note: If replication lag exceeds this limit, the replication slot will be invalidated by PostgreSQL."
                        );
                    }
                }
            } catch (SQLException ignored) {
                // Setting may not be available on older PostgreSQL versions (< 13)
            }
        }
    }
}
