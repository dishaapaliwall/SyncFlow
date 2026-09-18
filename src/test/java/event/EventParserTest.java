package event;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventParserTest {

    @Test
    void shouldParseEvent() {

        Map<String, Object> data = new HashMap<>();
        data.put("customer_id", 1);
        data.put("name", "Aarav Sharma");

        LocalDateTime timestamp = LocalDateTime.now();

        EventParser parser = new EventParser();

        ChangeEvent event = parser.parse(
                "customers",
                OperationType.INSERT,
                data,
                "0/16B3748",
                timestamp
        );

        assertEquals("customers", event.getTableName());
        assertEquals(OperationType.INSERT, event.getOperation());
        assertEquals(data, event.getData());
        assertEquals("0/16B3748", event.getLsn());
        assertEquals(timestamp, event.getTimestamp());
    }
}