package com.syncflow.cli;

import com.syncflow.cli.StopCommand;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class StopCommandTest {

    @Test
    void shouldExecuteStopCommand() {

        StopCommand command = new StopCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}