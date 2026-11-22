package com.syncflow.sync;

import com.syncflow.event.ChangeEvent;
import com.syncflow.event.OperationType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class TargetWriterTest {

    @Test
    void shouldInsertCustomer() throws Exception {

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        when(connection.prepareStatement(anyString()))
                .thenReturn(statement);

        Map<String, Object> data = new HashMap<>();
        data.put("customer_id", 101);
        data.put("name", "Test User");
        data.put("email", "test@gmail.com");
        data.put("phone", "9876543210");

        ChangeEvent event = new ChangeEvent(
                "customers",
                OperationType.INSERT,
                data,
                "0/123456",
                LocalDateTime.now()
        );

        TargetWriter writer = new TargetWriter(connection);

        writer.write(event);

        verify(connection).prepareStatement(anyString());
        verify(statement, times(4)).setObject(anyInt(), any());
        verify(statement).executeUpdate();
    }

    @Test
    void shouldInsertCustomerWithTableMapping() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);

        Map<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("name", "Mapped User");

        ChangeEvent event = new ChangeEvent(
                "source_customers",
                OperationType.INSERT,
                data,
                "0/123456",
                LocalDateTime.now()
        );

        TargetWriter writer = new TargetWriter(connection, Map.of("source_customers", "target_customers"));
        writer.write(event);

        verify(connection).prepareStatement(anyString());
        verify(statement, times(2)).setObject(anyInt(), any());
        verify(statement).executeUpdate();
        assertTrue(sqlCaptor.getValue().contains("\"target_customers\""));
    }

    @Test
    void shouldUpdateCustomerWithTableMapping() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);

        Map<String, Object> newData = Map.of("id", 1, "name", "Updated Name");
        Map<String, Object> oldData = Map.of("id", 1, "name", "Old Name");

        ChangeEvent event = new ChangeEvent(
                "source_customers",
                OperationType.UPDATE,
                newData,
                oldData,
                "0/123457",
                LocalDateTime.now()
        );

        TargetWriter writer = new TargetWriter(connection);
        writer.setTableMapping(Map.of("source_customers", "target_customers"));
        writer.write(event);

        verify(statement).executeUpdate();
        assertTrue(sqlCaptor.getValue().contains("\"target_customers\""));
    }

    @Test
    void shouldDeleteCustomerWithTableMapping() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);

        org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);

        Map<String, Object> oldData = Map.of("id", 1, "name", "Old Name");

        ChangeEvent event = new ChangeEvent(
                "source_customers",
                OperationType.DELETE,
                oldData,
                null,
                "0/123458",
                LocalDateTime.now()
        );

        TargetWriter writer = new TargetWriter(connection, Map.of("source_customers", "target_customers"));
        writer.write(event);

        verify(statement).executeUpdate();
        assertTrue(sqlCaptor.getValue().contains("\"target_customers\""));
    }
}
