package cli;

import picocli.CommandLine.Command;

@Command(
        name = "stop",
        description = "Stop SyncFlow CDC process"
)
public class StopCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("SyncFlow stopped");
    }
}