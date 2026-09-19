package sync;

import cdc.CdcConnection;
import event.ChangeEvent;
import monitoring.LagMonitor;
import monitoring.ProgressTracker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SyncEngineTest {

    @Test
    void shouldStartAndStopEngineGracefully() throws InterruptedException {

        CdcConnection cdcConnection = mock(CdcConnection.class);
        ProgressTracker tracker = new ProgressTracker();
        LagMonitor lagMonitor = new LagMonitor(300000);
        List<ChangeEvent> receivedEvents = new ArrayList<>();

        SyncEngine engine = new SyncEngine(
                cdcConnection,
                "syncflow_slot",
                "syncflow_publication",
                receivedEvents::add,
                tracker,
                lagMonitor
        );

        assertFalse(engine.isRunning());

        engine.start();
        assertTrue(engine.isRunning());

        Thread.sleep(200);

        engine.stop();
        assertFalse(engine.isRunning());
    }
}