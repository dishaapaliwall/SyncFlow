package sync;

import cdc.CdcConnection;
import cdc.CdcReader;
import cdc.LogicalDecoder;
import event.ChangeEvent;
import monitoring.LagMonitor;
import monitoring.ProgressTracker;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Consumer;

/**
 * The core orchestration engine that continuously reads from PostgreSQL CDC stream,
 * decodes change events, tracks replication lag and progress,
 * and forwards events to the consumer.
 */
public class SyncEngine implements Runnable {

    private final CdcConnection cdcConnection;
    private final String slotName;
    private final String publicationName;
    private final Consumer<ChangeEvent> eventConsumer;
    private final ProgressTracker progressTracker;
    private final LagMonitor lagMonitor;

    private volatile boolean running = false;
    private Thread workerThread;

    public SyncEngine(
            CdcConnection cdcConnection,
            String slotName,
            String publicationName,
            Consumer<ChangeEvent> eventConsumer,
            ProgressTracker progressTracker,
            LagMonitor lagMonitor
    ) {
        this.cdcConnection = cdcConnection;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.eventConsumer = eventConsumer;
        this.progressTracker = progressTracker;
        this.lagMonitor = lagMonitor;
    }

    /**
     * Starts the CDC synchronization engine in a background thread.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        workerThread = new Thread(this, "SyncFlow-Engine-Thread");
        workerThread.start();
    }

    /**
     * Stops the CDC synchronization engine gracefully.
     */
    public synchronized void stop() {
        running = false;
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {
        try {
            cdcConnection.connect();
            PGReplicationStream stream = cdcConnection.createReplicationStream(slotName, publicationName);
            CdcReader reader = new CdcReader(stream);
            LogicalDecoder decoder = new LogicalDecoder();

            while (running) {
                ByteBuffer buffer = reader.read();

                if (buffer == null) {
                    // No new WAL data available right now, sleep briefly
                    Thread.sleep(100);
                    continue;
                }

                ChangeEvent event = decoder.decode(buffer);

                if (event != null) {
                    // 1. Calculate replication lag (difference between event time and current time)
                    if (event.getTimestamp() != null && lagMonitor != null) {
                        long lagMillis = Math.abs(Duration.between(event.getTimestamp(), LocalDateTime.now()).toMillis());
                        lagMonitor.updateLag(lagMillis);
                    }

                    // 2. Forward event to consumer
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

                // 3. Acknowledge LSN to PostgreSQL
                reader.acknowledge();
            }

            reader.close();
            cdcConnection.close();

        } catch (InterruptedException e) {
            // Thread stopped gracefully
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("SyncEngine error: " + e.getMessage());
            running = false;
        }
    }
}