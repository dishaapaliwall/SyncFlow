package com.syncflow.snapshot.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class BootstrapPreflightValidatorUnitTest {

    private Connection mockSourceConn;
    private Connection mockTargetConn;
    private Statement mockSourceStmt;
    private Statement mockTargetStmt;
    private PreparedStatement mockSourcePs;
    private PreparedStatement mockTargetPs;
    private ResultSet mockSourceRs;
    private ResultSet mockTargetRs;

    private BootstrapPreflightValidator validator;

    @BeforeEach
    void setUp() throws SQLException {
        mockSourceConn = mock(Connection.class);
        mockTargetConn = mock(Connection.class);
        mockSourceStmt = mock(Statement.class);
        mockTargetStmt = mock(Statement.class);
        mockSourcePs = mock(PreparedStatement.class);
        mockTargetPs = mock(PreparedStatement.class);
        mockSourceRs = mock(ResultSet.class);
        mockTargetRs = mock(ResultSet.class);

        when(mockSourceConn.createStatement()).thenReturn(mockSourceStmt);
        when(mockTargetConn.createStatement()).thenReturn(mockTargetStmt);

        validator = new BootstrapPreflightValidator(
                "jdbc:postgresql://dummy:5432/src", "user", "pass",
                "jdbc:postgresql://dummy:5432/tgt", "user", "pass"
        );
    }

    // 1. Wal level validation
    @Test
    void testWalLevelLogicalPasses() throws SQLException {
        when(mockSourceStmt.executeQuery("SHOW wal_level")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getString(1)).thenReturn("logical");

        PreflightReport report = new PreflightReport();
        validator.validateWalLevel(mockSourceConn, report);

        assertTrue(report.isSuccess());
        assertTrue(report.getChecks().stream().anyMatch(c -> c.checkName().equals("WAL_LEVEL") && c.passed()));
    }

    @Test
    void testWalLevelReplicaFails() throws SQLException {
        when(mockSourceStmt.executeQuery("SHOW wal_level")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getString(1)).thenReturn("replica");

        PreflightReport report = new PreflightReport();
        validator.validateWalLevel(mockSourceConn, report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("must be 'logical'")));
    }

    // 2. Replication slot capacity & existing slot
    @Test
    void testSlotCapacityPassesWhenAvailable() throws SQLException {
        when(mockSourceStmt.executeQuery("SELECT current_setting('max_replication_slots')::int")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getInt(1)).thenReturn(10);

        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT slot_name FROM pg_replication_slots")).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("slot_name")).thenReturn("other_slot_1", "other_slot_2");

        PreflightReport report = new PreflightReport();
        validator.validateSlotCapacity(mockSourceConn, "new_test_slot", report);

        assertTrue(report.isSuccess());
        assertTrue(report.getChecks().stream().anyMatch(c -> c.checkName().equals("SLOT_CAPACITY") && c.passed()));
    }

    @Test
    void testSlotFailsWhenSlotAlreadyExists() throws SQLException {
        when(mockSourceStmt.executeQuery("SELECT current_setting('max_replication_slots')::int")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getInt(1)).thenReturn(10);

        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT slot_name FROM pg_replication_slots")).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString("slot_name")).thenReturn("existing_slot");

        PreflightReport report = new PreflightReport();
        validator.validateSlotCapacity(mockSourceConn, "existing_slot", report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("already exists")));
    }

    @Test
    void testSlotFailsWhenCapacityExhausted() throws SQLException {
        when(mockSourceStmt.executeQuery("SELECT current_setting('max_replication_slots')::int")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getInt(1)).thenReturn(2);

        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT slot_name FROM pg_replication_slots")).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString("slot_name")).thenReturn("slot1", "slot2");

        PreflightReport report = new PreflightReport();
        validator.validateSlotCapacity(mockSourceConn, "new_slot", report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("Insufficient replication slot capacity")));
    }

    // 3. Publication validation
    @Test
    void testPublicationPassesForTableMembership() throws SQLException {
        PreparedStatement psPub = mock(PreparedStatement.class);
        ResultSet rsPub = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT puballtables FROM pg_publication WHERE pubname = ?")).thenReturn(psPub);
        when(psPub.executeQuery()).thenReturn(rsPub);
        when(rsPub.next()).thenReturn(true);
        when(rsPub.getBoolean("puballtables")).thenReturn(false);

        PreparedStatement psTables = mock(PreparedStatement.class);
        ResultSet rsTables = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT tablename FROM pg_publication_tables WHERE pubname = ? AND schemaname = 'public'")).thenReturn(psTables);
        when(psTables.executeQuery()).thenReturn(rsTables);
        when(rsTables.next()).thenReturn(true, false);
        when(rsTables.getString("tablename")).thenReturn("test_table");

        PreflightReport report = new PreflightReport();
        validator.validatePublication(mockSourceConn, "test_pub", Set.of("test_table"), report);

        assertTrue(report.isSuccess());
    }

    @Test
    void testPublicationPassesForAllTables() throws SQLException {
        PreparedStatement psPub = mock(PreparedStatement.class);
        ResultSet rsPub = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT puballtables FROM pg_publication WHERE pubname = ?")).thenReturn(psPub);
        when(psPub.executeQuery()).thenReturn(rsPub);
        when(rsPub.next()).thenReturn(true);
        when(rsPub.getBoolean("puballtables")).thenReturn(true);

        PreflightReport report = new PreflightReport();
        validator.validatePublication(mockSourceConn, "all_pub", Set.of("any_table"), report);

        assertTrue(report.isSuccess());
    }

    @Test
    void testPublicationFailsWhenMissingTable() throws SQLException {
        PreparedStatement psPub = mock(PreparedStatement.class);
        ResultSet rsPub = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT puballtables FROM pg_publication WHERE pubname = ?")).thenReturn(psPub);
        when(psPub.executeQuery()).thenReturn(rsPub);
        when(rsPub.next()).thenReturn(true);
        when(rsPub.getBoolean("puballtables")).thenReturn(false);

        PreparedStatement psTables = mock(PreparedStatement.class);
        ResultSet rsTables = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement("SELECT tablename FROM pg_publication_tables WHERE pubname = ? AND schemaname = 'public'")).thenReturn(psTables);
        when(psTables.executeQuery()).thenReturn(rsTables);
        when(rsTables.next()).thenReturn(false); // empty

        PreflightReport report = new PreflightReport();
        validator.validatePublication(mockSourceConn, "test_pub", Set.of("missing_table"), report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("does not include requested source table(s)")));
    }

    // 5. Replica identity tests
    @Test
    void testReplicaIdentityDefaultWithPkPasses() throws SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement(contains("FROM pg_class c"))).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("relreplident")).thenReturn("d");
        when(rs.getInt("pk_count")).thenReturn(1);

        PreflightReport report = new PreflightReport();
        validator.validateReplicaIdentity(mockSourceConn, Set.of("orders"), report);

        assertTrue(report.isSuccess());
    }

    @Test
    void testReplicaIdentityDefaultWithoutPkFails() throws SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement(contains("FROM pg_class c"))).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("relreplident")).thenReturn("d");
        when(rs.getInt("pk_count")).thenReturn(0);

        PreflightReport report = new PreflightReport();
        validator.validateReplicaIdentity(mockSourceConn, Set.of("orders"), report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("DEFAULT replica identity but no primary key")));
    }

    @Test
    void testReplicaIdentityFullPasses() throws SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement(contains("FROM pg_class c"))).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("relreplident")).thenReturn("f");

        PreflightReport report = new PreflightReport();
        validator.validateReplicaIdentity(mockSourceConn, Set.of("orders"), report);

        assertTrue(report.isSuccess());
    }

    @Test
    void testReplicaIdentityIndexPassesWhenIndexExists() throws SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement(contains("FROM pg_class c"))).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("relreplident")).thenReturn("i");
        when(rs.getInt("replident_index_count")).thenReturn(1);

        PreflightReport report = new PreflightReport();
        validator.validateReplicaIdentity(mockSourceConn, Set.of("orders"), report);

        assertTrue(report.isSuccess());
    }

    @Test
    void testReplicaIdentityNothingFails() throws SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(mockSourceConn.prepareStatement(contains("FROM pg_class c"))).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getString("relreplident")).thenReturn("n");

        PreflightReport report = new PreflightReport();
        validator.validateReplicaIdentity(mockSourceConn, Set.of("orders"), report);

        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("REPLICA IDENTITY NOTHING")));
    }

    // 8. Table mapping validation
    @Test
    void testTableMappingFailsOnEmpty() {
        PreflightReport report = new PreflightReport();
        validator.validateTableMapping(Map.of(), report);
        assertFalse(report.isSuccess());
    }

    @Test
    void testTableMappingFailsOnInvalidIdentifier() {
        PreflightReport report = new PreflightReport();
        validator.validateTableMapping(Map.of("valid_table", "drop table;--"), report);
        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("not a valid SQL identifier")));
    }

    @Test
    void testTableMappingFailsOnDuplicateTarget() {
        PreflightReport report = new PreflightReport();
        validator.validateTableMapping(Map.of("src1", "same_target", "src2", "same_target"), report);
        assertFalse(report.isSuccess());
        assertTrue(report.getErrors().stream().anyMatch(e -> e.contains("Multiple source tables map to the same target table")));
    }

    // 9. WAL retention risk check
    @Test
    void testWalRetentionUnboundedAddsWarning() throws SQLException {
        when(mockSourceStmt.executeQuery("SHOW max_slot_wal_keep_size")).thenReturn(mockSourceRs);
        when(mockSourceRs.next()).thenReturn(true);
        when(mockSourceRs.getString(1)).thenReturn("-1");

        PreflightReport report = new PreflightReport();
        validator.validateWalRetentionRisks(mockSourceConn, report);

        assertTrue(report.isSuccess()); // Warning does not fail preflight
        assertTrue(report.getWarnings().stream().anyMatch(w -> w.contains("unbounded")));
    }
}
