package com.syncflow.cli;

import picocli.CommandLine.Command;

@Command(
        name = "start",
        description = "Start the CDC replication engine"
)
public class StartCommand implements Runnable {

    public StartCommand() {
    }

    @Override
    public void run() {
        System.out.println("Sending START request to SyncFlow engine...");
        String response = ApiClient.post("/start");
        System.out.println("Response: " + response);
    }
}