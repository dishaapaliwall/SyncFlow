package com.syncflow.cli;

import picocli.CommandLine.Command;

@Command(
        name = "status",
        description = "Show SyncFlow engine and connection status"
)
public class StatusCommand implements Runnable {

    public StatusCommand() {
    }

    @Override
    public void run() {
        String response = ApiClient.get("/status");
        System.out.println("SyncFlow Status: " + response);
    }
}