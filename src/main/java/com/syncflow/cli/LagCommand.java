package com.syncflow.cli;

import com.syncflow.monitoring.LagMonitor;
import picocli.CommandLine.Command;

@Command(
        name = "lag",
        description = "Show SyncFlow replication lag"
)
public class LagCommand implements Runnable {

    private final LagMonitor lagMonitor;

    public LagCommand() {
        this.lagMonitor = null;
    }

    public LagCommand(LagMonitor lagMonitor) {
        this.lagMonitor = lagMonitor;
    }

    @Override
    public void run() {
        if (lagMonitor != null) {
            System.out.println("Current lag: " + lagMonitor.getCurrentLagMillis() + " ms");
            if (lagMonitor.isAlertRequired()) {
                System.out.println("ALERT: Replication lag is above threshold");
            } else {
                System.out.println("Replication lag is within threshold");
            }
        } else {
            String response = ApiClient.get("/lag");
            System.out.println("Replication Lag: " + response);
        }
    }
}