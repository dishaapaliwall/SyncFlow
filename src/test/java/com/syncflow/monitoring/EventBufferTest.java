
package com.syncflow.monitoring;

import com.syncflow.event.ChangeEvent;
import com.syncflow.event.OperationType;
import com.syncflow.monitoring.EventBuffer;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventBufferTest {

    private ChangeEvent createEvent(String tableName) {
        return new ChangeEvent(
                tableName,
                OperationType.INSERT,
                Map.of("id", "1"),
                "0/100",
                LocalDateTime.now()
        );
    }

    @Test
    void shouldKeepOnlyMostRecentEvents() {
        EventBuffer buffer = new EventBuffer(2);

        buffer.add(createEvent("customers"));
        buffer.add(createEvent("orders"));
        buffer.add(createEvent("payments"));

        assertEquals(2, buffer.size());
        assertEquals(
                "orders",
                buffer.getRecentEvents().get(0).getTableName()
        );
        assertEquals(
                "payments",
                buffer.getRecentEvents().get(1).getTableName()
        );
    }

    @Test
    void shouldRejectInvalidCapacity() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new EventBuffer(0)
        );
    }

    @Test
    void shouldRejectNullEvent() {
        EventBuffer buffer = new EventBuffer(2);

        assertThrows(
                IllegalArgumentException.class,
                () -> buffer.add(null)
        );
    }
}
