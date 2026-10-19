
package com.syncflow.sync;

import com.syncflow.cdc.CdcConnection;
import com.syncflow.cdc.CdcReader;
import com.syncflow.cdc.LogicalDecoder;
import com.syncflow.event.ChangeEvent;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Consumer;

public class SyncEngine implements Runnable {

    private static final long INITIAL_RETRY_DELAY_MS = 1000;
    private static final long MAX_RETRY_DELAY_MS = 30000;

    private final CdcConnection cdcConnection;
    private final String slotName;
    private final String publicationName;
    private final Consumer<ChangeEvent> eventConsumer;
    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;

    private volatile boolean running = false;
    private volatile boolean connected = false;
    private volatile CdcReader activeReader;
    private Thread workerThread;

    public SyncEngine(
            CdcConnection cdcConnection,
            String slotName,
            String publicationName,
            Consumer<ChangeEvent> eventConsumer,
            ProgressTracker progressTracker,
            LagMonitor lagMonitor) {

        this.cdcConnection = cdcConnection;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.eventConsumer = eventConsumer;
        this.progressTracker = progressTracker;
        this.lagMonitor = lagMonitor;
    }

    public synchronized void start() {
        if (running || (workerThread != null && workerThread.isAlive())) {
            return;
        }

        running = true;
        workerThread = new Thread(this, "SyncFlow-Engine-Thread");
        workerThread.start();
    }

    public void stop() {
        Thread threadToStop;

        synchronized (this) {
            running = false;
            threadToStop = workerThread;

            if (threadToStop != null) {
                threadToStop.interrupt();
            }
        }

        // Closing the active stream helps unblock a worker reading from it.
        CdcReader reader = activeReader;
        if (reader != null) {
            try {
                reader.close();
            } catch (SQLException e) {
                System.err.println(
                        "Error closing replication stream during stop: "
                                + e.getMessage());
            }
        }

        if (threadToStop != null
                && threadToStop != Thread.currentThread()) {
            try {
                threadToStop.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void run() {
        long retryDelayMs = INITIAL_RETRY_DELAY_MS;

        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                CdcReader reader = null;
                connected = false;
                try {
                    System.out.println("Connecting to source PostgreSQL...");

                    cdcConnection.connect();

                    if (!running) {
                        break;
                    }

                    PGReplicationStream stream =
                            cdcConnection.createReplicationStream(
                                    slotName, publicationName);

                    reader = new CdcReader(stream);
                    activeReader = reader;
                    connected = true;

                    System.out.println("CDC replication stream connected.");

                    // Reset the delay after a successful connection.
                    retryDelayMs = INITIAL_RETRY_DELAY_MS;
                    System.out.println("CDC replication stream connected.");

                    LogicalDecoder decoder = new LogicalDecoder();

                    while (running
                            && !Thread.currentThread().isInterrupted()) {
                        ByteBuffer buffer = reader.read();

                        if (buffer == null) {
                            Thread.sleep(100);
                            continue;
                        }

                        ChangeEvent event = decoder.decode(buffer);

                        if (event != null) {
                            if (event.getTimestamp() != null
                                    && lagMonitor != null) {
                                long lagMillis = Math.abs(
                                        Duration.between(
                                                event.getTimestamp(),
                                                LocalDateTime.now()
                                        ).toMillis());

                                lagMonitor.updateLag(lagMillis);
                            }

                            try {
                                if (eventConsumer != null) {
                                    eventConsumer.accept(event);
                                }

                                if (progressTracker != null) {
                                    progressTracker.recordSuccess();
                                }
                            } catch (Exception e) {
                                if (progressTracker != null) {
                                    progressTracker.recordFailure();
                                }
                                throw e;
                            }
                        }

                        reader.acknowledge();
                    }

                } catch (InterruptedException e) {
                    // Manual stop requested. Do not retry.
                    Thread.currentThread().interrupt();
                    break;

                } catch (Exception e) {
                    if (!running
                            || Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    System.err.println(
                            "CDC connection/stream failed: "
                                    + e.getMessage());

                } finally {
                    activeReader = null;
                    connected = false;

                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (SQLException e) {
                            System.err.println(
                                    "Error closing replication stream: "
                                            + e.getMessage());
                        }
                    }

                    try {
                        cdcConnection.close();
                    } catch (SQLException e) {
                        System.err.println(
                                "Error closing CDC connection: "
                                        + e.getMessage());
                    }
                }

                if (!running || Thread.currentThread().isInterrupted()) {
                    break;
                }

                System.out.println(
                        "Retrying CDC connection in "
                                + retryDelayMs + " ms...");

                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                retryDelayMs = Math.min(
                        retryDelayMs * 2,
                        MAX_RETRY_DELAY_MS);
            }

        } finally {
            running = false;
            activeReader = null;
            System.out.println("CDC engine stopped.");
        }
    }
}
