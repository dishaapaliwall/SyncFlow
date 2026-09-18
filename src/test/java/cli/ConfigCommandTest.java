package cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class ConfigCommandTest {

    @Test
    void shouldAcceptSourceAndTargetOptions() {

        ConfigCommand command = new ConfigCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute(
                "--source",
                "jdbc:postgresql://localhost:5434/syncflow_source",
                "--target",
                "jdbc:postgresql://localhost:5435/syncflow_target"
        );

        assertEquals(0, exitCode);
    }

    @Test
    void shouldRunWithoutConfiguration() {

        ConfigCommand command = new ConfigCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}