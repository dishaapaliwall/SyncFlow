
package com.syncflow.service;

import com.syncflow.cdc.CdcConnection;
import com.syncflow.monitoring.EventBuffer;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import com.syncflow.sync.BatchProcessor;
import com.syncflow.sync.SyncEngine;
import com.syncflow.sync.TargetWriter;
import com.syncflow.sync.TransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Value;

import com.syncflow.reliability.CheckpointManager;

@Service
public class SyncService {

    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;
    private final SyncEngine syncEngine;
    private final EventBuffer eventBuffer;
    private final Connection targetConnection;
    private Properties createConnectionProperties(
            String username, String password) {
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

        CdcConnection cdcConnection =
                new CdcConnection(sourceUrl, sourceUsername, sourcePassword);

        this.targetConnection = DriverManager.getConnection(
                targetUrl,
                createConnectionProperties(targetUsername, targetPassword)
        );

        TargetWriter targetWriter = new TargetWriter(targetConnection);

        TransactionManager transactionManager =
                new TransactionManager(targetConnection);

        CheckpointManager checkpointManager =
                new CheckpointManager(targetConnection);

        BatchProcessor batchProcessor =
                new BatchProcessor(
                        targetWriter,
                        transactionManager,
                        checkpointManager
                );

        this.progressTracker = new ProgressTracker();
        this.lagMonitor = new LagMonitor(300000);
        this.eventBuffer = new EventBuffer(100);


        this.syncEngine = new SyncEngine(
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
                checkpointManager
        );
    }

    public synchronized void start() {
        if (!syncEngine.isRunning()) {
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

}
