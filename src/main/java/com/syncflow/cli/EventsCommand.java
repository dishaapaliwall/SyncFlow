package com.syncflow.cli;

import picocli.CommandLine.Command;

@Command(
        name = "events",
        description = "Show recent captured CDC change events"
)
public class EventsCommand implements Runnable {

    public EventsCommand() {
    }

    @Override
    public void run() {
        String response = ApiClient.get("/events");
        System.out.println("Recent Events: " + response);
    }
}