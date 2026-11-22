package com.syncflow.snapshot;

import com.syncflow.reliability.CheckpointManager;
import com.syncflow.sync.TransactionManager;

import java.util.List;
import java.util.Map;

public class BootstrapManager {

    private final SnapshotManager snapshotManager;
    private final SnapshotWorker snapshotWorker;
    private final SnapshotStateManager stateManager;
    private final CheckpointManager checkpointManager;
    private final TransactionManager transactionManager;

    public BootstrapManager(
            SnapshotManager snapshotManager,
            SnapshotWorker snapshotWorker,
            SnapshotStateManager stateManager,
            CheckpointManager checkpointManager,
            TransactionManager transactionManager
    ) {
        this.snapshotManager = snapshotManager;
        this.snapshotWorker = snapshotWorker;
        this.stateManager = stateManager;
        this.checkpointManager = checkpointManager;
        this.transactionManager = transactionManager;
    }

    public void startBootstrap(
            String slotName,
            Map<String, String> tables
    ) throws Exception {
        startBootstrap(slotName, null, tables);
    }

    public void startBootstrap(
            String slotName,
            String publicationName,
            Map<String, String> tables
    ) throws Exception {

        String currentStatus = stateManager.getBootstrapStatus();
        if ("CATCHING_UP".equalsIgnoreCase(currentStatus) || "STREAMING".equalsIgnoreCase(currentStatus)) {
            throw new IllegalStateException(
                    "Cannot start fresh bootstrap: initial snapshot has already been completed (status: "
                            + currentStatus + "). Target tables already contain bootstrap data. Resume CDC instead."
            );
        }

        try {
            System.out.println("[BOOTSTRAP] Starting initial snapshot...");

            stateManager.recordSlotAndPublication(slotName, publicationName);

            // 1. Create exported snapshot.
            SnapshotManager.SnapshotInfo snapshot =
                    snapshotManager.createExportedSnapshot(slotName);

            System.out.println(
                    "[BOOTSTRAP] Snapshot LSN: " +
                            snapshot.consistentPoint()
            );

            // 2. Register bootstrap state.
            stateManager.beginBootstrap(
                    snapshot.consistentPoint(),
                    tables
            );

            // 3. Copy all tables using the same snapshot.
            for (Map.Entry<String, String> entry : tables.entrySet()) {

                String sourceTable = entry.getKey();
                String targetTable = entry.getValue();

                System.out.println(
                        "[BOOTSTRAP] Copying " + sourceTable
                );

                snapshotWorker.copyTable(
                        snapshot.snapshotName(),
                        sourceTable,
                        targetTable
                );
            }

            // 4. Release the snapshot exporter after copying.
            snapshotManager.release();

            // 5. Persist the snapshot LSN as the CDC starting checkpoint.
            transactionManager.begin();

            try {
                checkpointManager.saveCheckpoint(
                        snapshot.consistentPoint()
                );

                transactionManager.commit();

            } catch (Exception e) {
                transactionManager.rollback();
                throw e;
            }

            System.out.println(
                    "[BOOTSTRAP] Snapshot checkpoint saved: " +
                            snapshot.consistentPoint()
            );

            // 6. Transition to CDC catch-up.
            stateManager.markCatchingUp();

            System.out.println(
                    "[BOOTSTRAP] Snapshot completed. Entering CATCHING_UP."
            );

        } catch (Exception e) {

            stateManager.markFailed(e.getMessage());

            System.err.println(
                    "[BOOTSTRAP] Failed: " + e.getMessage()
            );

            throw e;

        } finally {
            snapshotManager.release();
        }
    }
}