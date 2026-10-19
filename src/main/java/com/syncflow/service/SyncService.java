
package com.syncflow.service;

import com.syncflow.cdc.CdcConnection;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import com.syncflow.sync.SyncEngine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.syncflow.monitoring.EventBuffer;

@Service
public class SyncService {

    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;
    private final SyncEngine syncEngine;
    private final EventBuffer eventBuffer;

    public SyncService(
            @Value("${syncflow.source.url:jdbc:postgresql://localhost:5434/syncflow_source}")
            String sourceUrl,

            @Value("${syncflow.source.username:syncflow}")
            String username,

            @Value("${syncflow.source.password:syncflow123}")
            String password,

            @Value("${syncflow.cdc.slot:syncflow_slot}")
            String slotName,

            @Value("${syncflow.cdc.publication:syncflow_publication}")
            String publicationName
    ) {
        CdcConnection cdcConnection =
                new CdcConnection(sourceUrl, username, password);

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
                                    + " | LSN: " + event.getLsn()
                    );
                },
                progressTracker,
                lagMonitor
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
