
package com.syncflow.service;

import com.syncflow.cdc.CdcConnection;
import com.syncflow.monitoring.EventBuffer;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import com.syncflow.reliability.CheckpointManager;
import com.syncflow.snapshot.BootstrapManager;
import com.syncflow.snapshot.BootstrapRecoveryManager;
import com.syncflow.snapshot.SnapshotManager;
import com.syncflow.snapshot.SnapshotStateManager;
import com.syncflow.snapshot.SnapshotWorker;
import com.syncflow.snapshot.validation.BootstrapPreflightValidator;
import com.syncflow.snapshot.validation.PreflightReport;
import com.syncflow.sync.BatchProcessor;
import com.syncflow.sync.SyncEngine;
import com.syncflow.sync.TargetWriter;
import com.syncflow.sync.TransactionManager;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

@Service
public class SyncService implements AutoCloseable {

    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;
    private final EventBuffer eventBuffer;

    private final Connection targetConnection;
    private final CdcConnection cdcConnection;

    private final CheckpointManager checkpointManager;
    private final TransactionManager transactionManager;
    private final BatchProcessor batchProcessor;
    private final TargetWriter targetWriter;

    private final String sourceUrl;
    private final String sourceUsername;
    private final String sourcePassword;

    private final String targetUrl;
    private final String targetUsername;
    private final String targetPassword;

    private final String slotName;
    private final String publicationName;

    private SyncEngine syncEngine;
    private BootstrapPreflightValidator preflightValidator;

    private Properties createConnectionProperties(
            String username,
            String password
    ) {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("options", "-c TimeZone=UTC");
        return properties;
    }

    public SyncService(
            @Value("${syncflow.source.url}") String sourceUrl,
            @Value("${syncflow.source.username}") String sourceUsername,
            @Value("${syncflow.source.password}") String sourcePassword,
            @Value("${syncflow.cdc.slot}") String slotName,
            @Value("${syncflow.cdc.publication}") String publicationName,
            @Value("${syncflow.target.url}") String targetUrl,
            @Value("${syncflow.target.username}") String targetUsername,
            @Value("${syncflow.target.password}") String targetPassword
    ) throws SQLException {

        this.sourceUrl = sourceUrl;
        this.sourceUsername = sourceUsername;
        this.sourcePassword = sourcePassword;

        this.targetUrl = targetUrl;
        this.targetUsername = targetUsername;
        this.targetPassword = targetPassword;

        this.slotName = slotName;
        this.publicationName = publicationName;

        this.cdcConnection =
                new CdcConnection(
                        sourceUrl,
                        sourceUsername,
                        sourcePassword
                );

        this.targetConnection = DriverManager.getConnection(
                targetUrl,
                createConnectionProperties(
                        targetUsername,
                        targetPassword
                )
        );

        this.targetWriter =
                new TargetWriter(targetConnection);

        this.transactionManager =
                new TransactionManager(targetConnection);

        this.checkpointManager =
                new CheckpointManager(targetConnection);

        this.batchProcessor =
                new BatchProcessor(
                        targetWriter,
                        transactionManager,
                        checkpointManager
                );

        this.progressTracker = new ProgressTracker();
        this.lagMonitor = new LagMonitor(300000);
        this.eventBuffer = new EventBuffer(100);

        // Existing CDC startup remains available.
        this.syncEngine = createSyncEngine(null);
    }

    private SyncEngine createSyncEngine(String startingLsn) {

        return new SyncEngine(
                cdcConnection,
                slotName,
                publicationName,
                event -> {
                    eventBuffer.add(event);

                    System.out.println(
                            "[CDC-EVENT] " + event.getOperation()
                                    + " | Table: " + event.getTableName()
                                    + " | Data: " + event.getData()
                                    + " | Old data: " + event.getOldData()
                                    + " | LSN: " + event.getLsn()
                    );
                },
                progressTracker,
                lagMonitor,
                batchProcessor,
                checkpointManager,
                startingLsn
        );
    }

    // Existing normal CDC startup.
    public synchronized void start() {
        if (!syncEngine.isRunning()) {
            syncEngine.start();
        }
    }

    // New startup path: snapshot first, then CDC with restart recovery.
    public synchronized void bootstrapAndStart(
            Map<String, String> tables
    ) throws Exception {

        if (syncEngine.isRunning()) {
            throw new IllegalStateException(
                    "Stop CDC before starting bootstrap."
            );
        }

        // Configure CDC TargetWriter with the requested table mapping.
        if (targetWriter != null) {
            targetWriter.setTableMapping(tables);
        }

        // Separate connection for bootstrap state management.
        // It must not share the CDC/checkpoint transaction connection.
        try (Connection stateConnection = DriverManager.getConnection(
                targetUrl,
                createConnectionProperties(targetUsername, targetPassword)
        )) {

            SnapshotStateManager stateManager =
                    new SnapshotStateManager(stateConnection);

            stateManager.initialize();

            BootstrapRecoveryManager recoveryManager = new BootstrapRecoveryManager(
                    sourceUrl, sourceUsername, sourcePassword,
                    targetUrl, targetUsername, targetPassword,
                    stateManager, checkpointManager
            );

            BootstrapRecoveryManager.RecoveryAction action = recoveryManager.determineAction(tables);
            System.out.println("[SYNC-SERVICE] Determined bootstrap action: " + action);

            // SCENARIO B & C: Snapshot already completed (CATCHING_UP or STREAMING).
            // Do NOT run snapshot, do NOT truncate target tables, do NOT drop slot.
            if (action == BootstrapRecoveryManager.RecoveryAction.RESUME_CATCHING_UP
                    || action == BootstrapRecoveryManager.RecoveryAction.RESUME_STREAMING) {

                String resumeLsn = recoveryManager.validateAndPrepareResume(
                        slotName, publicationName, tables
                );

                System.out.println(
                        "[SYNC-SERVICE] Resuming CDC from checkpoint LSN: " + resumeLsn
                );

                syncEngine = createSyncEngine(resumeLsn);
                syncEngine.start();
                return;
            }

            // SCENARIO A: Crash occurred during previous snapshot (COPYING or FAILED).
            // Safely abandon incomplete snapshot, drop stale slot, truncate only registered target tables.
            if (action == BootstrapRecoveryManager.RecoveryAction.RETRY_SNAPSHOT) {
                recoveryManager.recoverIncompleteSnapshot(slotName, tables);
            }

            // Fresh start (NEW) or recovered fresh snapshot: Run preflight validations.
            BootstrapPreflightValidator validator = preflightValidator != null
                    ? preflightValidator
                    : new BootstrapPreflightValidator(
                            sourceUrl, sourceUsername, sourcePassword,
                            targetUrl, targetUsername, targetPassword
                    );

            PreflightReport report = validator.validate(slotName, publicationName, tables);
            report.logReport();

            System.out.println(
                    "[SYNC-SERVICE] Starting initial bootstrap..."
            );

            SnapshotManager snapshotManager =
                    new SnapshotManager(
                            sourceUrl,
                            sourceUsername,
                            sourcePassword
                    );

            SnapshotWorker snapshotWorker =
                    new SnapshotWorker(
                            sourceUrl,
                            targetUrl,
                            sourceUsername,
                            sourcePassword,
                            stateManager
                    );

            BootstrapManager bootstrapManager =
                    new BootstrapManager(
                            snapshotManager,
                            snapshotWorker,
                            stateManager,
                            checkpointManager,
                            transactionManager
                    );

            // Complete snapshot and save its LSN checkpoint.
            bootstrapManager.startBootstrap(
                    slotName,
                    publicationName,
                    tables
            );

            // Retrieve the LSN established by the bootstrap.
            String snapshotLsn = stateManager.getSnapshotLsn();

            if (snapshotLsn == null || snapshotLsn.isBlank()) {
                throw new IllegalStateException(
                        "Bootstrap completed without a snapshot LSN."
                );
            }

            System.out.println(
                    "[SYNC-SERVICE] Starting CDC from snapshot LSN: "
                            + snapshotLsn
            );

            // Start CDC from the same slot and snapshot LSN.
            syncEngine = createSyncEngine(snapshotLsn);
            syncEngine.start();
        }
    }


    public synchronized void stop() {
        syncEngine.stop();
    }

    public boolean isRunning() {
        return syncEngine.isRunning();
    }

    public boolean isConnected() {
        return syncEngine.isConnected();
    }

    public ProgressTracker getProgressTracker() {
        return progressTracker;
    }

    public LagMonitor getLagMonitor() {
        return lagMonitor;
    }

    public EventBuffer getEventBuffer() {
        return eventBuffer;
    }

    public BootstrapPreflightValidator getPreflightValidator() {
        return preflightValidator;
    }

    public TargetWriter getTargetWriter() {
        return targetWriter;
    }

    public void setPreflightValidator(BootstrapPreflightValidator preflightValidator) {
        this.preflightValidator = preflightValidator;
    }

    public void markStreaming() throws SQLException {
        try (Connection stateConnection = DriverManager.getConnection(
                targetUrl,
                createConnectionProperties(targetUsername, targetPassword)
        )) {
            SnapshotStateManager stateManager = new SnapshotStateManager(stateConnection);
            stateManager.markStreaming();
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        stop();

        if (cdcConnection != null) {
            try {
                cdcConnection.close();
            } catch (SQLException ignored) {}
        }

        if (targetConnection != null && !targetConnection.isClosed()) {
            targetConnection.close();
        }
    }
}
