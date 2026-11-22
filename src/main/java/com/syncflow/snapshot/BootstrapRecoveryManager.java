package com.syncflow.snapshot;

import com.syncflow.reliability.CheckpointManager;
import org.postgresql.replication.LogSequenceNumber;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class BootstrapRecoveryManager {

    private static final Pattern VALID_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    public enum RecoveryAction {
        START_FRESH,
        RETRY_SNAPSHOT,
        RESUME_CATCHING_UP,
        RESUME_STREAMING
    }

    private final String sourceUrl;
    private final String sourceUsername;
    private final String sourcePassword;
    private final String targetUrl;
    private final String targetUsername;
    private final String targetPassword;
    private final SnapshotStateManager stateManager;
    private final CheckpointManager checkpointManager;

    public BootstrapRecoveryManager(
            String sourceUrl,
            String sourceUsername,
            String sourcePassword,
            String targetUrl,
            String targetUsername,
            String targetPassword,
            SnapshotStateManager stateManager,
            CheckpointManager checkpointManager
    ) {
        this.sourceUrl = sourceUrl;
        this.sourceUsername = sourceUsername;
        this.sourcePassword = sourcePassword;
        this.targetUrl = targetUrl;
        this.targetUsername = targetUsername;
        this.targetPassword = targetPassword;
        this.stateManager = stateManager;
        this.checkpointManager = checkpointManager;
    }

    private Properties createProperties(String username, String password) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("options", "-c TimeZone=UTC");
        return props;
    }

    public RecoveryAction determineAction() throws SQLException {
        return determineAction(null);
    }

    /**
     * Inspects persistent state and table states to determine the safe startup or recovery action.
     * Safely distinguishes complete snapshots from incomplete snapshots even when status is FAILED.
     */
    public RecoveryAction determineAction(Map<String, String> currentTables) throws SQLException {
        String status = stateManager.getBootstrapStatus();
        if (status == null || "NEW".equalsIgnoreCase(status)) {
            return RecoveryAction.START_FRESH;
        } else if ("COPYING".equalsIgnoreCase(status)) {
            return RecoveryAction.RETRY_SNAPSHOT;
        } else if ("CATCHING_UP".equalsIgnoreCase(status)) {
            return RecoveryAction.RESUME_CATCHING_UP;
        } else if ("STREAMING".equalsIgnoreCase(status)) {
            return RecoveryAction.RESUME_STREAMING;
        } else if ("FAILED".equalsIgnoreCase(status)) {
            return classifyFailedState(currentTables);
        } else {
            throw new IllegalStateException("Unknown bootstrap status in metadata: " + status);
        }
    }

    /**
     * Classifies a FAILED status:
     * 1. Inspects persisted table_state records.
     * 2. Verifies every registered table has status COPIED.
     * 3. Verifies the persisted table mapping is complete.
     * 4. Retrieves the durable checkpoint.
     * 5. Verifies checkpoint is valid and numerically >= snapshot LSN.
     * If all pass -> RESUME_CATCHING_UP.
     * If demonstrably incomplete -> RETRY_SNAPSHOT.
     * If ambiguous/inconsistent -> fails safely rather than truncating target data.
     */
    private RecoveryAction classifyFailedState(Map<String, String> currentTables) throws SQLException {
        Map<String, String> tableStatuses = stateManager.getTableStatuses();
        String snapshotLsnStr = stateManager.getSnapshotLsn();
        String checkpointLsnStr = checkpointManager != null ? checkpointManager.getLastProcessedLsn() : null;

        if (tableStatuses.isEmpty() && (snapshotLsnStr == null || snapshotLsnStr.isBlank())) {
            // Snapshot never actually started
            return RecoveryAction.RETRY_SNAPSHOT;
        }

        boolean allTablesCopied = !tableStatuses.isEmpty()
                && tableStatuses.values().stream().allMatch("COPIED"::equalsIgnoreCase);

        boolean hasIncompleteTable = tableStatuses.values().stream()
                .anyMatch(s -> "PENDING".equalsIgnoreCase(s) || "COPYING".equalsIgnoreCase(s) || "FAILED".equalsIgnoreCase(s));

        // Check if current tables match table_state
        boolean mappingComplete = true;
        if (currentTables != null && !currentTables.isEmpty()) {
            for (String srcTable : currentTables.keySet()) {
                if (!tableStatuses.containsKey(srcTable) || !"COPIED".equalsIgnoreCase(tableStatuses.get(srcTable))) {
                    mappingComplete = false;
                    break;
                }
            }
        }

        if (allTablesCopied && mappingComplete && snapshotLsnStr != null && !snapshotLsnStr.isBlank()
                && checkpointLsnStr != null && !checkpointLsnStr.isBlank()) {

            LogSequenceNumber checkpointLsn = LogSequenceNumber.valueOf(checkpointLsnStr);
            LogSequenceNumber snapshotLsn = LogSequenceNumber.valueOf(snapshotLsnStr);

            if (checkpointLsn.compareTo(snapshotLsn) >= 0) {
                // Completed snapshot! CDC catch-up failed after snapshot committed.
                System.out.println("[RECOVERY] FAILED status detected, but all tables are COPIED and checkpoint ("
                        + checkpointLsnStr + " >= " + snapshotLsnStr + ") is valid. Resuming CATCHING_UP safely.");
                return RecoveryAction.RESUME_CATCHING_UP;
            } else {
                throw new IllegalStateException(
                        "Inconsistent recovery metadata for FAILED status: all tables are COPIED, but checkpoint LSN ("
                                + checkpointLsnStr + ") is earlier than snapshot LSN (" + snapshotLsnStr
                                + "). Manual intervention required to prevent accidental data loss."
                );
            }
        }

        if (hasIncompleteTable || !mappingComplete || checkpointLsnStr == null || checkpointLsnStr.isBlank()) {
            // Demonstrably incomplete snapshot
            System.out.println("[RECOVERY] FAILED status detected with incomplete snapshot copying. Retrying snapshot.");
            return RecoveryAction.RETRY_SNAPSHOT;
        }

        throw new IllegalStateException(
                "Ambiguous recovery metadata for FAILED status. Table statuses: " + tableStatuses
                        + ", snapshot LSN: " + snapshotLsnStr + ", checkpoint LSN: " + checkpointLsnStr
                        + ". Manual intervention required to prevent accidental data loss."
        );
    }

    /**
     * Scenario A: Crash occurred while status was COPYING or demonstrably incomplete FAILED.
     * Safely abandons the incomplete snapshot, drops the verified replication slot,
     * resets only the registered SyncFlow-owned target tables, and resets bootstrap state to NEW.
     */
    public void recoverIncompleteSnapshot(
            String slotName,
            Map<String, String> tables
    ) throws SQLException {
        System.out.println("[RECOVERY] Executing Scenario A recovery: Resetting incomplete snapshot...");

        // 1. Validate table mapping consistency before destructive cleanup
        validateTableMappingConsistency(tables);

        // 2. Validate slot ownership before dropping
        validateSlotOwnershipForDrop(slotName);

        // 3. Drop the verified stale replication slot on source DB if it exists
        dropSlotIfExists(slotName);

        // 4. Reset ONLY the explicitly registered, SyncFlow-owned target tables using RESTRICT
        resetTargetTables(tables.values());

        // 5. Reset persistent bootstrap and table states back to NEW
        stateManager.resetToNew();

        System.out.println("[RECOVERY] Incomplete snapshot cleanup complete. Ready for fresh bootstrap.");
    }

    /**
     * Scenario B & C: Crash occurred after snapshot completed (in CATCHING_UP or STREAMING).
     * Validates configuration consistency and ensures replication slot and publication exist.
     * Never drops or recreates the slot, and never truncates target tables.
     *
     * @return the durable starting LSN for CDC
     */
    public String validateAndPrepareResume(
            String slotName,
            String publicationName,
            Map<String, String> tables
    ) throws SQLException {
        String status = stateManager.getBootstrapStatus();
        System.out.println("[RECOVERY] Validating resume conditions for status: " + status);

        // 1. Validate configuration consistency against persisted metadata
        validateSlotConsistency(slotName);
        validatePublicationConsistency(publicationName);
        validateTableMappingConsistency(tables);

        // 2. Verify replication slot exists on PostgreSQL and belongs to SyncFlow
        validateSlotExistsAndMatches(slotName);

        // 3. Verify publication exists and includes tables
        validatePublication(publicationName, tables.keySet());

        // 4. Retrieve durable checkpoint LSN and validate numerically against snapshot LSN
        String checkpointLsnStr = checkpointManager != null ? checkpointManager.getLastProcessedLsn() : null;
        String snapshotLsnStr = stateManager.getSnapshotLsn();

        if (checkpointLsnStr == null || checkpointLsnStr.isBlank()) {
            if (snapshotLsnStr == null || snapshotLsnStr.isBlank()) {
                throw new IllegalStateException(
                        "No valid checkpoint or snapshot LSN found in target metadata for " + status + " recovery."
                );
            }
            checkpointLsnStr = snapshotLsnStr;
        }

        LogSequenceNumber checkpointLsn = LogSequenceNumber.valueOf(checkpointLsnStr);
        if (snapshotLsnStr != null && !snapshotLsnStr.isBlank()) {
            LogSequenceNumber snapshotLsn = LogSequenceNumber.valueOf(snapshotLsnStr);
            if (checkpointLsn.compareTo(snapshotLsn) < 0) {
                throw new IllegalStateException(
                        "Checkpoint LSN (" + checkpointLsnStr + ") is earlier than snapshot LSN ("
                                + snapshotLsnStr + "). Checkpoint is invalid for recovery."
                );
            }
        }

        System.out.println("[RECOVERY] Validated resume from checkpoint LSN: " + checkpointLsnStr);
        return checkpointLsnStr;
    }

    /**
     * Resets ONLY explicitly registered target tables using TRUNCATE TABLE ... RESTRICT.
     * Never uses CASCADE, never touches unregistered tables.
     * If an unregistered table references a managed table, PostgreSQL rejects the operation.
     */
    public void resetTargetTables(Collection<String> targetTables) throws SQLException {
        if (targetTables == null || targetTables.isEmpty()) {
            return;
        }

        StringJoiner joiner = new StringJoiner(", ");
        for (String table : targetTables) {
            if (table == null || !VALID_IDENTIFIER.matcher(table).matches()) {
                throw new IllegalArgumentException("Invalid SQL identifier for target table: " + table);
            }
            joiner.add("public.\"" + table + "\"");
        }

        String sql = "TRUNCATE TABLE " + joiner + " RESTRICT";
        System.out.println("[RECOVERY] Truncating registered target tables: " + joiner + " RESTRICT");

        Properties props = createProperties(targetUsername, targetPassword);
        try (Connection conn = DriverManager.getConnection(targetUrl, props);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SQLException(
                    "Failed to reset target tables. An external or unregistered table may have a foreign key "
                            + "referencing one of the managed tables, preventing safe truncation: " + e.getMessage(),
                    e.getSQLState(),
                    e.getErrorCode(),
                    e
            );
        }
    }

    /**
     * Strengthened slot ownership validation:
     * - Requires a nonblank persisted slot name.
     * - Requires exact equality between persisted slot name and configured slot name.
     * - If slot exists on source DB but ownership cannot be established, fails without deleting it.
     */
    public void validateSlotOwnershipForDrop(String slotName) throws SQLException {
        if (slotName == null || !VALID_IDENTIFIER.matcher(slotName).matches()) {
            throw new IllegalArgumentException("Invalid slot name: " + slotName);
        }

        String recordedSlot = stateManager.getSlotName();
        if (recordedSlot == null || recordedSlot.isBlank()) {
            if (slotExistsOnServer(slotName)) {
                throw new IllegalStateException(
                        "Cannot drop replication slot '" + slotName + "': slot exists on source database, "
                                + "but bootstrap metadata has no recorded slot ownership. "
                                + "Will not delete an unverified slot."
                );
            }
            return;
        }

        if (!recordedSlot.equals(slotName)) {
            throw new IllegalStateException(
                    "Replication slot mismatch: Bootstrap state recorded slot '" + recordedSlot
                            + "', but current configuration is '" + slotName + "'. Will not drop unrelated slot."
            );
        }
    }

    public void validateSlotConsistency(String slotName) throws SQLException {
        String recordedSlot = stateManager.getSlotName();
        if (recordedSlot != null && !recordedSlot.isBlank() && !recordedSlot.equals(slotName)) {
            throw new IllegalStateException(
                    "Configured replication slot '" + slotName
                            + "' does not match persisted bootstrap slot '" + recordedSlot + "'."
            );
        }
    }

    public void validatePublicationConsistency(String publicationName) throws SQLException {
        String recordedPub = stateManager.getPublicationName();
        if (recordedPub != null && !recordedPub.isBlank() && !recordedPub.equals(publicationName)) {
            throw new IllegalStateException(
                    "Configured publication '" + publicationName
                            + "' does not match persisted bootstrap publication '" + recordedPub + "'."
            );
        }
    }

    public void validateTableMappingConsistency(Map<String, String> currentTables) throws SQLException {
        if (currentTables == null || currentTables.isEmpty()) {
            return;
        }
        Map<String, String> persistedMapping = stateManager.getTableMapping();
        if (persistedMapping != null && !persistedMapping.isEmpty()) {
            if (!persistedMapping.equals(currentTables)) {
                throw new IllegalStateException(
                        "Configured table mapping " + currentTables
                                + " does not match persisted bootstrap table mapping " + persistedMapping + "."
                );
            }
        }
    }

    public boolean slotExistsOnServer(String slotName) throws SQLException {
        Properties props = createProperties(sourceUsername, sourcePassword);
        try (Connection conn = DriverManager.getConnection(sourceUrl, props);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void dropSlotIfExists(String slotName) throws SQLException {
        Properties props = createProperties(sourceUsername, sourcePassword);
        try (Connection conn = DriverManager.getConnection(sourceUrl, props);
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT plugin, active FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
                if (!rs.next()) {
                    return; // Slot does not exist
                }
                String plugin = rs.getString("plugin");
                boolean active = rs.getBoolean("active");

                if (!"pgoutput".equals(plugin)) {
                    throw new IllegalStateException(
                            "Replication slot '" + slotName + "' has unexpected plugin '" + plugin
                                    + "'. Will not drop non-SyncFlow slot."
                    );
                }
                if (active) {
                    throw new IllegalStateException(
                            "Replication slot '" + slotName + "' is currently active for another process. Cannot drop."
                    );
                }
            }

            stmt.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            System.out.println("[RECOVERY] Dropped abandoned replication slot: " + slotName);
        }
    }

    public void validateSlotExistsAndMatches(String slotName) throws SQLException {
        if (slotName == null || !VALID_IDENTIFIER.matcher(slotName).matches()) {
            throw new IllegalArgumentException("Invalid slot name: " + slotName);
        }

        Properties props = createProperties(sourceUsername, sourcePassword);
        try (Connection conn = DriverManager.getConnection(sourceUrl, props);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT plugin FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "Replication slot '" + slotName + "' does not exist on source database during recovery. "
                                    + "Cannot resume CDC because WAL from consistent point is lost. Manual intervention required."
                    );
                }
                String plugin = rs.getString("plugin");
                if (!"pgoutput".equals(plugin)) {
                    throw new IllegalStateException(
                            "Replication slot '" + slotName + "' has unexpected plugin '" + plugin
                                    + "'. Expected 'pgoutput'."
                    );
                }
            }
        }
    }

    private void validatePublication(String publicationName, Set<String> sourceTables) throws SQLException {
        if (publicationName == null || !VALID_IDENTIFIER.matcher(publicationName).matches()) {
            throw new IllegalArgumentException("Invalid publication name: " + publicationName);
        }

        Properties props = createProperties(sourceUsername, sourcePassword);
        try (Connection conn = DriverManager.getConnection(sourceUrl, props)) {
            boolean pubAllTables = false;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT puballtables FROM pg_publication WHERE pubname = ?")) {
                ps.setString(1, publicationName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException(
                                "Publication '" + publicationName + "' does not exist on source database during recovery."
                        );
                    }
                    pubAllTables = rs.getBoolean("puballtables");
                }
            }

            if (!pubAllTables) {
                Set<String> published = new HashSet<>();
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT tablename FROM pg_publication_tables WHERE pubname = ? AND schemaname = 'public'")) {
                    ps.setString(1, publicationName);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            published.add(rs.getString("tablename"));
                        }
                    }
                }

                for (String table : sourceTables) {
                    if (!published.contains(table)) {
                        throw new IllegalStateException(
                                "Publication '" + publicationName + "' does not include required source table '"
                                        + table + "' during recovery."
                        );
                    }
                }
            }
        }
    }
}
