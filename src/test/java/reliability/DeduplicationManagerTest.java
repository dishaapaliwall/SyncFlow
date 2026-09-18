package reliability;

import event.ChangeEvent;
import event.OperationType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeduplicationManagerTest {

    @Test
    void shouldDetectDuplicateEvent() {

        DeduplicationManager manager =
                new DeduplicationManager();

        ChangeEvent event = createEvent("0/123456");

        assertFalse(manager.isDuplicate(event));
        assertTrue(manager.isDuplicate(event));
    }

    @Test
    void shouldAllowDifferentEvents() {

        DeduplicationManager manager =
                new DeduplicationManager();

        ChangeEvent event1 = createEvent("0/123456");
        ChangeEvent event2 = createEvent("0/123457");

        assertFalse(manager.isDuplicate(event1));
        assertFalse(manager.isDuplicate(event2));
    }

    @Test
    void shouldRejectNullEvent() {

        DeduplicationManager manager =
                new DeduplicationManager();

        assertThrows(
                IllegalArgumentException.class,
                () -> manager.isDuplicate(null)
        );
    }

    private ChangeEvent createEvent(String lsn) {

        return new ChangeEvent(
                "customers",
                OperationType.INSERT,
                Map.of(
                        "customer_id", 1,
                        "name", "Test User",
                        "email", "test@gmail.com"
                ),
                lsn,
                LocalDateTime.now()
        );
    }
}