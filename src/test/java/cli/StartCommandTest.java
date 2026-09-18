package cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class StartCommandTest {

    @Test
    void shouldExecuteStartCommand() {

        StartCommand command = new StartCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}