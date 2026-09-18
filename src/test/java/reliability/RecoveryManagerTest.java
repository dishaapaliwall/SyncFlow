package reliability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RecoveryManagerTest {

    @Test
    void shouldRecoverFromCheckpoint() {

        CheckpointManager checkpointManager =
                new CheckpointManager();

        checkpointManager.saveCheckpoint("0/123456");

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertEquals(
                "0/123456",
                recoveryManager.recover()
        );
    }

    @Test
    void shouldAllowRecoveryWhenCheckpointExists() {

        CheckpointManager checkpointManager =
                new CheckpointManager();

        checkpointManager.saveCheckpoint("0/123456");

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertTrue(recoveryManager.canRecover());
    }

    @Test
    void shouldNotRecoverWithoutCheckpoint() {

        CheckpointManager checkpointManager =
                new CheckpointManager();

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertFalse(recoveryManager.canRecover());
        assertNull(recoveryManager.recover());
    }
}