
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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.syncflow.event.SourceTransaction;
import org.postgresql.replication.LogSequenceNumber;

import com.syncflow.reliability.CheckpointManager;

public class SyncEngine implements Runnable {

    private static final long INITIAL_RETRY_DELAY_MS = 1000;
    private static final long MAX_RETRY_DELAY_MS = 30000;

    private final CdcConnection cdcConnection;
    private final String slotName;
    private final String publicationName;
    private final Consumer<ChangeEvent> eventConsumer;
    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;
    private final BatchProcessor batchProcessor;

    private final CheckpointManager checkpointManager;


    private volatile boolean running;
    private volatile boolean connected;
    private volatile CdcReader activeReader;
    private Thread workerThread;

    public SyncEngine(
            CdcConnection cdcConnection,
            String slotName,
            String publicationName,
            Consumer<ChangeEvent> eventConsumer,
            ProgressTracker progressTracker,
            LagMonitor lagMonitor,
            BatchProcessor batchProcessor,
            CheckpointManager checkpointManager) {

        this.cdcConnection = cdcConnection;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.eventConsumer = eventConsumer;
        this.progressTracker = progressTracker;
        this.lagMonitor = lagMonitor;
        this.batchProcessor = batchProcessor;
        this.checkpointManager = checkpointManager;
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
        running = false;

        Thread thread = workerThread;
        if (thread != null) {
            thread.interrupt();
        }

        CdcReader reader = activeReader;
        if (reader != null) {
            try {
                reader.close();
            } catch (SQLException e) {
                System.err.println("Error closing CDC stream: " + e.getMessage());
            }
        }

        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(3000);
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
        long retryDelay = INITIAL_RETRY_DELAY_MS;

        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                CdcReader reader = null;

                try {
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
                    retryDelay = INITIAL_RETRY_DELAY_MS;

                    LogicalDecoder decoder = new LogicalDecoder();
                    List<ChangeEvent> transactionEvents = new ArrayList<>();

                    while (running && !Thread.currentThread().isInterrupted()) {
                        ByteBuffer buffer = reader.read();

                        if (buffer == null) {
                            Thread.sleep(100);
                            continue;
                        }

                        ChangeEvent event = decoder.decode(buffer);

                        if (event != null) {
                            transactionEvents.add(event);

                            if (event.getTimestamp() != null && lagMonitor != null) {
                                long lag = Math.abs(Duration.between(
                                        event.getTimestamp(),
                                        LocalDateTime.now()).toMillis());
                                lagMonitor.updateLag(lag);
                            }
                        }

                        if (decoder.isCommitMessage()) {

                            // Build one complete source transaction.
                            SourceTransaction sourceTransaction = new SourceTransaction(
                                    decoder.getCommitLsn(),
                                    decoder.getEndLsn(),
                                    decoder.getCommitTimestamp(),
                                    transactionEvents
                            );

                            String endLsn = sourceTransaction.getEndLsn();

                            System.out.println(
                                    "[SyncFlow] COMMIT received - commitLsn: "
                                            + sourceTransaction.getCommitLsn()
                                            + ", endLsn: " + endLsn
                            );

                            // Check the last transaction successfully committed to the target.
                            String savedCheckpoint = checkpointManager.getLastProcessedLsn();

                            boolean alreadyApplied = savedCheckpoint != null
                                    && LogSequenceNumber.valueOf(endLsn).compareTo(
                                    LogSequenceNumber.valueOf(savedCheckpoint)
                            ) <= 0;

                            if (alreadyApplied) {

                                // The target already contains this transaction.
                                // Do not apply it again.
                                System.out.println(
                                        "[SyncFlow] Skipping already-applied transaction. End LSN: "
                                                + endLsn
                                );

                            } else {

                                // Apply data and save checkpoint atomically.
                                batchProcessor.process(sourceTransaction);



                                // Update monitoring only for newly applied transactions.
                                for (ChangeEvent changeEvent : transactionEvents) {
                                    eventConsumer.accept(changeEvent);
                                    progressTracker.recordSuccess();
                                }
                            }


                            // Acknowledge only after the transaction is known to be applied.
                            reader.acknowledge(LogSequenceNumber.valueOf(endLsn));

                            System.out.println(
                                    "[SyncFlow] Acknowledged source transaction end LSN: "
                                            + endLsn
                            );

                            // Clear after successful processing and acknowledgement.
                            transactionEvents.clear();
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    if (!running || Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    if (progressTracker != null) {
                        progressTracker.recordFailure();
                    }

                    System.err.println("CDC sync failed: " + e.getMessage());
                } finally {
                    connected = false;
                    activeReader = null;

                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (SQLException e) {
                            System.err.println("Error closing CDC stream: " + e.getMessage());
                        }
                    }

                    try {
                        cdcConnection.close();
                    } catch (SQLException e) {
                        System.err.println("Error closing source connection: " + e.getMessage());
                    }
                }

                if (!running || Thread.currentThread().isInterrupted()) {
                    break;
                }

                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MS);
            }
        } finally {
            connected = false;
            running = false;
            activeReader = null;
        }
    }
}
