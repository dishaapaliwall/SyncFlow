package com.syncflow.reliability;

import java.sql.SQLException;

public class RecoveryManager {

    private final CheckpointManager checkpointManager;

    public RecoveryManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    public String recover() {

        try {
            return checkpointManager.getLastProcessedLsn();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to retrieve recovery checkpoint", e);
        }
    }

    public boolean canRecover() {

        try {
            return checkpointManager.getLastProcessedLsn() != null;
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to check recovery checkpoint", e);
        }
    }
}