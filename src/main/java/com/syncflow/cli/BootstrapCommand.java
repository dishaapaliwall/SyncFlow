package com.syncflow.cli;

import picocli.CommandLine.Command;

@Command(
        name = "bootstrap",
        description = "Copy existing source data to target first, then start CDC"
)
public class BootstrapCommand implements Runnable {

    public BootstrapCommand() {
    }

    @Override
    public void run() {
        System.out.println("Sending BOOTSTRAP request to SyncFlow (Snapshot existing data + CDC)...");
        String response = ApiClient.post("/bootstrap");
        System.out.println("Response: " + response);
    }
}