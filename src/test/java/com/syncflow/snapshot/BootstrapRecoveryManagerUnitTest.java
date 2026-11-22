package com.syncflow.snapshot;

import com.syncflow.reliability.CheckpointManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class BootstrapRecoveryManagerUnitTest {

    private SnapshotStateManager stateManager;
    private CheckpointManager checkpointManager;
    private BootstrapRecoveryManager recoveryManager;

    @BeforeEach
    void setUp() {
        stateManager = mock(SnapshotStateManager.class);
        checkpointManager = mock(CheckpointManager.class);

        recoveryManager = new BootstrapRecoveryManager(
                "jdbc:postgresql://dummy:5432/src", "user", "pass",
                "jdbc:postgresql://dummy:5432/tgt", "user", "pass",
                stateManager, checkpointManager
        );
    }

    @Test
    void testDetermineActionReturnsStartFreshForNew() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("NEW");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.START_FRESH, recoveryManager.determineAction(Map.of("t1", "t1")));
    }

    @Test
    void testDetermineActionReturnsRetrySnapshotForCopying() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("COPYING");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.RETRY_SNAPSHOT, recoveryManager.determineAction(Map.of("t1", "t1")));
    }

    @Test
    void testDetermineActionReturnsResumeCatchingUpForCatchingUp() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("CATCHING_UP");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.RESUME_CATCHING_UP, recoveryManager.determineAction(Map.of("t1", "t1")));
    }

    @Test
    void testDetermineActionReturnsResumeStreamingForStreaming() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("STREAMING");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.RESUME_STREAMING, recoveryManager.determineAction(Map.of("t1", "t1")));
    }

    @Test
    void testDetermineActionThrowsOnUnknownStatus() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("CORRUPTED");
        assertThrows(IllegalStateException.class, () -> recoveryManager.determineAction(Map.of("t1", "t1")));
    }

    // FAILED state classification tests
    @Test
    void testFailedStateWithCompletedSnapshotClassifiesAsResumeCatchingUp() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("FAILED");
        when(stateManager.getTableStatuses()).thenReturn(Map.of("customers", "COPIED", "orders", "COPIED"));
        when(stateManager.getSnapshotLsn()).thenReturn("0/1000");
        when(checkpointManager.getLastProcessedLsn()).thenReturn("0/1200"); // checkpoint >= snapshot

        Map<String, String> currentTables = Map.of("customers", "target_customers", "orders", "target_orders");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.RESUME_CATCHING_UP, recoveryManager.determineAction(currentTables));
    }

    @Test
    void testFailedStateWithIncompleteTableStatesClassifiesAsRetrySnapshot() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("FAILED");
        when(stateManager.getTableStatuses()).thenReturn(Map.of("customers", "COPIED", "orders", "FAILED"));
        when(stateManager.getSnapshotLsn()).thenReturn("0/1000");
        when(checkpointManager.getLastProcessedLsn()).thenReturn(null);

        Map<String, String> currentTables = Map.of("customers", "target_customers", "orders", "target_orders");
        assertEquals(BootstrapRecoveryManager.RecoveryAction.RETRY_SNAPSHOT, recoveryManager.determineAction(currentTables));
    }

    @Test
    void testFailedStateWithOlderCheckpointThrowsAmbiguousException() throws SQLException {
        when(stateManager.getBootstrapStatus()).thenReturn("FAILED");
        when(stateManager.getTableStatuses()).thenReturn(Map.of("customers", "COPIED"));
        when(stateManager.getSnapshotLsn()).thenReturn("0/2000");
        when(checkpointManager.getLastProcessedLsn()).thenReturn("0/1000"); // checkpoint earlier than snapshot

        Map<String, String> currentTables = Map.of("customers", "target_customers");
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> recoveryManager.determineAction(currentTables));
        assertTrue(ex.getMessage().contains("earlier than snapshot LSN"));
    }

    // Table reset safety tests
    @Test
    void testResetTargetTablesRejectsInvalidIdentifier() {
        assertThrows(IllegalArgumentException.class, () ->
                recoveryManager.resetTargetTables(List.of("valid_table", "drop table customers;--"))
        );
    }

    @Test
    void testResetTargetTablesHandlesEmptyGracefully() throws SQLException {
        assertDoesNotThrow(() -> recoveryManager.resetTargetTables(List.of()));
        assertDoesNotThrow(() -> recoveryManager.resetTargetTables(null));
    }

    // Slot ownership validation tests
    @Test
    void testValidateSlotOwnershipRejectsMismatchedSlot() throws SQLException {
        when(stateManager.getSlotName()).thenReturn("other_recorded_slot");
        assertThrows(IllegalStateException.class, () ->
                recoveryManager.validateSlotOwnershipForDrop("different_slot")
        );
    }

    // Configuration consistency validation tests
    @Test
    void testSlotConsistencyRejectsMismatch() throws SQLException {
        when(stateManager.getSlotName()).thenReturn("persisted_slot");
        assertThrows(IllegalStateException.class, () ->
                recoveryManager.validateSlotConsistency("different_configured_slot")
        );
    }

    @Test
    void testPublicationConsistencyRejectsMismatch() throws SQLException {
        when(stateManager.getPublicationName()).thenReturn("persisted_pub");
        assertThrows(IllegalStateException.class, () ->
                recoveryManager.validatePublicationConsistency("different_configured_pub")
        );
    }

    @Test
    void testTableMappingConsistencyRejectsMismatch() throws SQLException {
        when(stateManager.getTableMapping()).thenReturn(Map.of("t1", "tgt1", "t2", "tgt2"));
        assertThrows(IllegalStateException.class, () ->
                recoveryManager.validateTableMappingConsistency(Map.of("t1", "tgt1", "t3", "tgt3"))
        );
    }
}
