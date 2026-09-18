package cli;

import monitoring.ProgressTracker;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.*;

class ProgressCommandTest {

    @Test
    void shouldExecuteProgressCommand() {

        ProgressTracker tracker =
                new ProgressTracker();

        tracker.recordSuccess();
        tracker.recordSuccess();
        tracker.recordFailure();

        ProgressCommand command =
                new ProgressCommand(tracker);

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }

    @Test
    void shouldExecuteWithEmptyProgress() {

        ProgressCommand command =
                new ProgressCommand();

        CommandLine commandLine =
                new CommandLine(command);

        int exitCode = commandLine.execute();

        assertEquals(0, exitCode);
    }
}