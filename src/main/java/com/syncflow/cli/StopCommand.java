package com.syncflow.cli;

import picocli.CommandLine.Command;

@Command(
        name = "stop",
        description = "Stop the CDC replication engine"
)
public class StopCommand implements Runnable {

    public StopCommand() {
    }

    @Override
    public void run() {
        System.out.println("Sending STOP request to SyncFlow engine...");
        String response = ApiClient.post("/stop");
        System.out.println("Response: " + response);
    }
}