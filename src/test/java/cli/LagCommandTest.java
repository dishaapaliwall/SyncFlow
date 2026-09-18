package cli;

import monitoring.LagMonitor;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class LagCommandTest {

    @Test
    void shouldExecuteLagCommand() {

        LagMonitor monitor =
                new LagMonitor(300000);

        monitor.updateLag(1000);

        LagCommand command =
                new LagCommand(monitor);

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }

    @Test
    void shouldExecuteWithDefaultMonitor() {

        LagCommand command =
                new LagCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }

    @Test
    void shouldExecuteWhenLagExceedsThreshold() {

        LagMonitor monitor =
                new LagMonitor(300000);

        monitor.updateLag(400000);

        LagCommand command =
                new LagCommand(monitor);

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}