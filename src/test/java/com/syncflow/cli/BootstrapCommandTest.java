package com.syncflow.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BootstrapCommandTest {

    @Test
    void shouldExecuteBootstrapCommand() {
        BootstrapCommand command = new BootstrapCommand();

        CommandLine commandLine = new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}