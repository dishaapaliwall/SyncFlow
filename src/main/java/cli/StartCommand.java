package cli;

import cdc.CdcConnection;
import monitoring.LagMonitor;
import monitoring.ProgressTracker;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import sync.SyncEngine;

/**
 * CLI command to start the SyncFlow CDC engine.
 */
@Command(
        name = "start",
        description = "Start SyncFlow CDC process"
)
public class StartCommand implements Runnable {

    @Option(names = {"--source"}, description = "Source PostgreSQL JDBC URL", defaultValue = "jdbc:postgresql://localhost:5434/syncflow_source")
    private String sourceUrl = "jdbc:postgresql://localhost:5434/syncflow_source";

    @Option(names = {"--user"}, description = "Database username", defaultValue = "syncflow")
    private String username = "syncflow";

    @Option(names = {"--password"}, description = "Database password", defaultValue = "syncflow123")
    private String password = "syncflow123";

    @Option(names = {"--slot"}, description = "Replication slot name", defaultValue = "syncflow_slot")
    private String slotName = "syncflow_slot";

    @Option(names = {"--publication"}, description = "Publication name", defaultValue = "syncflow_publication")
    private String publicationName = "syncflow_publication";

    @Option(names = {"--mode"}, description = "Sync mode: 'continuous' or 'once'", defaultValue = "once")
    private String mode = "once";

    private SyncEngine syncEngine;

    public StartCommand() {
    }

    // Constructor for testing with mock engine
    public StartCommand(SyncEngine syncEngine) {
        this.syncEngine = syncEngine;
    }

    @Override
    public void run() {
        System.out.println("==========================================");
        System.out.println("         SYNCFLOW CDC ENGINE v1.0         ");
        System.out.println("==========================================");
        System.out.println("Source URL:    " + sourceUrl);
        System.out.println("Slot Name:     " + slotName);
        System.out.println("Publication:   " + publicationName);
        System.out.println("Execution Mode: " + mode);
        System.out.println("==========================================");

        if (!"continuous".equalsIgnoreCase(mode)) {
            System.out.println("SyncFlow started in test mode. Exiting.");
            return;
        }

        System.out.println("Starting continuous real-time CDC synchronization...");
        System.out.println("Press Ctrl+C to stop.\n");

        if (syncEngine == null) {
            CdcConnection cdcConnection = new CdcConnection(sourceUrl, username, password);
            ProgressTracker tracker = new ProgressTracker();
            LagMonitor lagMonitor = new LagMonitor(300000); // 5 minutes alert threshold

            syncEngine = new SyncEngine(
                    cdcConnection,
                    slotName,
                    publicationName,
                    event -> {
                        System.out.println("[CDC-EVENT] " + event.getOperation() +
                                " on table '" + event.getTableName() +
                                "' | Data: " + event.getData() +
                                " | LSN: " + event.getLsn() +
                                " | Time: " + event.getTimestamp());
                    },
                    tracker,
                    lagMonitor
            );
        }

        // Handle Ctrl+C gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down SyncFlow CDC engine...");
            syncEngine.stop();
        }));

        syncEngine.start();

        // Keep main thread alive while engine is running
        while (syncEngine.isRunning()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}