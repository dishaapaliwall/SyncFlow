
package com.syncflow.reliability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RecoveryManagerTest {

    @Test
    void shouldRecoverFromCheckpoint() throws Exception {

        CheckpointManager checkpointManager =
                mock(CheckpointManager.class);

        when(checkpointManager.getLastProcessedLsn())
                .thenReturn("0/123456");

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertEquals(
                "0/123456",
                recoveryManager.recover()
        );
    }

    @Test
    void shouldAllowRecoveryWhenCheckpointExists() throws Exception {

        CheckpointManager checkpointManager =
                mock(CheckpointManager.class);

        when(checkpointManager.getLastProcessedLsn())
                .thenReturn("0/123456");

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertTrue(recoveryManager.canRecover());
    }

    @Test
    void shouldNotRecoverWithoutCheckpoint() throws Exception {

        CheckpointManager checkpointManager =
                mock(CheckpointManager.class);

        when(checkpointManager.getLastProcessedLsn())
                .thenReturn(null);

        RecoveryManager recoveryManager =
                new RecoveryManager(checkpointManager);

        assertFalse(recoveryManager.canRecover());
        assertNull(recoveryManager.recover());
    }
}
