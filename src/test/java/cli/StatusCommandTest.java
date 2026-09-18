package cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class StatusCommandTest {

    @Test
    void shouldExecuteStatusCommand() {

        StatusCommand command = new StatusCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}