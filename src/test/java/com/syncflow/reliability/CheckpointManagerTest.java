
package com.syncflow.reliability;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CheckpointManagerTest {

    private CheckpointManager createManager(
            Connection connection,
            PreparedStatement statement
    ) throws Exception {

        when(connection.prepareStatement(anyString()))
                .thenReturn(statement);

        when(statement.executeUpdate()).thenReturn(1);

        return new CheckpointManager(connection);
    }

    @Test
    void shouldSaveCheckpoint() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        CheckpointManager manager =
                createManager(connection, statement);

        manager.saveCheckpoint("0/123456");

        verify(statement).setString(1, "0/123456");
        verify(statement, times(2)).executeUpdate();
    }

    @Test
    void shouldUpdateCheckpoint() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        CheckpointManager manager =
                createManager(connection, statement);

        manager.saveCheckpoint("0/123456");
        manager.saveCheckpoint("0/123457");

        verify(statement).setString(1, "0/123456");
        verify(statement).setString(1, "0/123457");
    }

    @Test
    void shouldRejectEmptyLsn() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        CheckpointManager manager =
                createManager(connection, statement);

        assertThrows(
                IllegalArgumentException.class,
                () -> manager.saveCheckpoint("")
        );
    }

    @Test
    void shouldInitiallyHaveNoCheckpoint() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(anyString()))
                .thenReturn(statement);

        when(statement.executeUpdate()).thenReturn(1);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("last_processed_lsn"))
                .thenReturn(null);

        CheckpointManager manager =
                new CheckpointManager(connection);

        assertNull(manager.getLastProcessedLsn());
    }

    @Test
    void shouldReadSavedCheckpointFromDatabase() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(anyString()))
                .thenReturn(statement);

        when(statement.executeUpdate()).thenReturn(1);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("last_processed_lsn"))
                .thenReturn("0/123456");

        CheckpointManager manager =
                new CheckpointManager(connection);

        assertEquals(
                "0/123456",
                manager.getLastProcessedLsn()
        );
    }
}
