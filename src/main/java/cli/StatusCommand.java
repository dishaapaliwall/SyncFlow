package cli;

import picocli.CommandLine.Command;

@Command(
        name = "status",
        description = "Show SyncFlow status"
)
public class StatusCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("SyncFlow status: RUNNING");
    }
}