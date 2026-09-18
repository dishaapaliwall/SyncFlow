package sync;

import event.ChangeEvent;
import event.OperationType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}