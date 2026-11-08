
package com.syncflow.sync;

import com.syncflow.event.ChangeEvent;
import com.syncflow.event.SourceTransaction;
import com.syncflow.reliability.CheckpointManager;

import java.sql.SQLException;
import java.util.List;

public class BatchProcessor {

    private final TargetWriter targetWriter;
    private final TransactionManager transactionManager;
    private final CheckpointManager checkpointManager;

    public BatchProcessor(
            TargetWriter targetWriter,
            TransactionManager transactionManager
    ) {
        this(targetWriter, transactionManager, null);
    }

    public BatchProcessor(
            TargetWriter targetWriter,
            TransactionManager transactionManager,
            CheckpointManager checkpointManager
    ) {
        this.targetWriter = targetWriter;
        this.transactionManager = transactionManager;
        this.checkpointManager = checkpointManager;
    }

    /**
     * Applies one complete source transaction and saves its checkpoint
     * atomically in the target database.
     */
    public void process(SourceTransaction sourceTransaction)
            throws SQLException {

        if (sourceTransaction == null) {
            throw new IllegalArgumentException(
                    "Source transaction cannot be null");
        }

        if (checkpointManager == null) {
            throw new IllegalStateException(
                    "CheckpointManager is not configured");
        }

        transactionManager.begin();

        try {
            for (ChangeEvent event : sourceTransaction.getEvents()) {
                targetWriter.write(event);
            }

            // Save checkpoint BEFORE committing target transaction.
            checkpointManager.saveCheckpoint(
                    sourceTransaction.getEndLsn()
            );

            // Data and checkpoint become permanent together.
            transactionManager.commit();

        } catch (SQLException | RuntimeException e) {

            try {
                transactionManager.rollback();
            } catch (SQLException rollbackError) {
                e.addSuppressed(rollbackError);
            }

            throw e;
        }
    }

    /**
     * Temporary compatibility method for existing tests and callers.
     * This method does not save a source transaction checkpoint.
     */
    @Deprecated
    public void process(List<ChangeEvent> events) throws SQLException {

        if (events == null || events.isEmpty()) {
            return;
        }

        transactionManager.begin();

        try {
            for (ChangeEvent event : events) {
                targetWriter.write(event);
            }

            transactionManager.commit();

        } catch (SQLException | RuntimeException e) {

            try {
                transactionManager.rollback();
            } catch (SQLException rollbackError) {
                e.addSuppressed(rollbackError);
            }

            throw e;
        }
    }
}
