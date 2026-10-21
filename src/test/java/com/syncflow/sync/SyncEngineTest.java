
package com.syncflow.sync;

import com.syncflow.cdc.CdcConnection;
import com.syncflow.event.ChangeEvent;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SyncEngineTest {

    @Test
    void shouldStartAndStopEngineGracefully() throws InterruptedException {

        CdcConnection cdcConnection = mock(CdcConnection.class);
        Connection targetConnection = mock(Connection.class);

        TargetWriter targetWriter = new TargetWriter(targetConnection);
        TransactionManager transactionManager =
                new TransactionManager(targetConnection);
        BatchProcessor batchProcessor =
                new BatchProcessor(targetWriter, transactionManager);

        ProgressTracker tracker = new ProgressTracker();
        LagMonitor lagMonitor = new LagMonitor(300000);
        List<ChangeEvent> receivedEvents = new ArrayList<>();

        SyncEngine engine = new SyncEngine(
                cdcConnection,
                "syncflow_slot",
                "syncflow_publication",
                receivedEvents::add,
                tracker,
                lagMonitor,
                batchProcessor
        );

        assertFalse(engine.isRunning());

        engine.start();
        assertTrue(engine.isRunning());

        Thread.sleep(200);

        engine.stop();
        assertFalse(engine.isRunning());

        //verify(cdcConnection, atLeastOnce()).connect();
    }
}
