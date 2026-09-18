package cli;

import monitoring.ProgressTracker;
import picocli.CommandLine.Command;

@Command(
        name = "progress",
        description = "Show SyncFlow progress"
)
public class ProgressCommand implements Runnable {

    private final ProgressTracker progressTracker;

    public ProgressCommand() {
        this.progressTracker = new ProgressTracker();
    }

    public ProgressCommand(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {

        System.out.println(
                "Total events: " +
                        progressTracker.getTotalEvents()
        );

        System.out.println(
                "Successful events: " +
                        progressTracker.getSuccessfulEvents()
        );

        System.out.println(
                "Failed events: " +
                        progressTracker.getFailedEvents()
        );

        System.out.println(
                "Success rate: " +
                        progressTracker.getSuccessRate() +
                        "%"
        );
    }
}