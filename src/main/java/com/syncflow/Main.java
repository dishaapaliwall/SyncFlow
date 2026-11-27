package com.syncflow;

import com.syncflow.cli.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Main application entry point for SyncFlow CLI.
 */
@Command(
        name = "syncflow",
        description = "SyncFlow: Real-Time Data Sync Between PostgreSQL Databases",
        mixinStandardHelpOptions = true,
        subcommands = {
                StartCommand.class,
                BootstrapCommand.class,
                StopCommand.class,
                StatusCommand.class,
                ProgressCommand.class,
                LagCommand.class,
                EventsCommand.class,
                ConfigCommand.class
        }
)
public class Main implements Runnable {

    @Override
    public void run() {
        System.out.println("Welcome to SyncFlow CLI! Use --help to see available commands.");
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}