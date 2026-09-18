package reliability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointManagerTest {

    @Test
    void shouldSaveCheckpoint() {

        CheckpointManager manager =
                new CheckpointManager();

        manager.saveCheckpoint("0/123456");

        assertEquals(
                "0/123456",
                manager.getLastProcessedLsn()
        );
    }

    @Test
    void shouldUpdateCheckpoint() {

        CheckpointManager manager =
                new CheckpointManager();

        manager.saveCheckpoint("0/123456");
        manager.saveCheckpoint("0/123457");

        assertEquals(
                "0/123457",
                manager.getLastProcessedLsn()
        );
    }

    @Test
    void shouldRejectEmptyLsn() {

        CheckpointManager manager =
                new CheckpointManager();

        assertThrows(
                IllegalArgumentException.class,
                () -> manager.saveCheckpoint("")
        );
    }

    @Test
    void shouldInitiallyHaveNoCheckpoint() {

        CheckpointManager manager =
                new CheckpointManager();

        assertNull(manager.getLastProcessedLsn());
    }
}