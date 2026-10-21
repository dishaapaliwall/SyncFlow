package com.syncflow.cli;

import com.syncflow.monitoring.ProgressTracker;
import picocli.CommandLine.Command;

@Command(
        name = "progress",
        description = "Show SyncFlow replication progress"
)
public class ProgressCommand implements Runnable {

    private final ProgressTracker progressTracker;

    public ProgressCommand() {
        this.progressTracker = null;
    }

    public ProgressCommand(ProgressTracker progressTracker) {
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        if (progressTracker != null) {
            System.out.println("Total events: " + progressTracker.getTotalEvents());
            System.out.println("Successful events: " + progressTracker.getSuccessfulEvents());
            System.out.println("Failed events: " + progressTracker.getFailedEvents());
            System.out.println("Success rate: " + progressTracker.getSuccessRate() + "%");
        } else {
            String response = ApiClient.get("/progress");
            System.out.println("Replication Progress: " + response);
        }
    }
}