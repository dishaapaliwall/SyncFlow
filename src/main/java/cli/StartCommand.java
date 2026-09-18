package cli;

import picocli.CommandLine.Command;

@Command(
        name = "start",
        description = "Start SyncFlow CDC process"
)
public class StartCommand implements Runnable {

    @Override
    public void run() {
        System.out.println("SyncFlow started");
    }
}