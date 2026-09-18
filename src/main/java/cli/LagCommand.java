package cli;

import monitoring.LagMonitor;
import picocli.CommandLine.Command;

@Command(
        name = "lag",
        description = "Show SyncFlow replication lag"
)
public class LagCommand implements Runnable {

    private final LagMonitor lagMonitor;

    public LagCommand() {
        this.lagMonitor = new LagMonitor(300000);
    }

    public LagCommand(LagMonitor lagMonitor) {
        this.lagMonitor = lagMonitor;
    }

    @Override
    public void run() {

        System.out.println(
                "Current lag: " +
                        lagMonitor.getCurrentLagMillis() +
                        " ms"
        );

        if (lagMonitor.isAlertRequired()) {
            System.out.println("ALERT: Replication lag is above threshold");
        } else {
            System.out.println("Replication lag is within threshold");
        }
    }
}