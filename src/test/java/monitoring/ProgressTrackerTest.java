package monitoring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProgressTrackerTest {

    @Test
    void shouldRecordSuccessfulEvents() {

        ProgressTracker tracker =
                new ProgressTracker();

        tracker.recordSuccess();
        tracker.recordSuccess();

        assertEquals(2, tracker.getTotalEvents());
        assertEquals(2, tracker.getSuccessfulEvents());
        assertEquals(0, tracker.getFailedEvents());
    }

    @Test
    void shouldRecordFailedEvents() {

        ProgressTracker tracker =
                new ProgressTracker();

        tracker.recordFailure();

        assertEquals(1, tracker.getTotalEvents());
        assertEquals(0, tracker.getSuccessfulEvents());
        assertEquals(1, tracker.getFailedEvents());
    }

    @Test
    void shouldCalculateSuccessRate() {

        ProgressTracker tracker =
                new ProgressTracker();

        tracker.recordSuccess();
        tracker.recordSuccess();
        tracker.recordFailure();

        assertEquals(
                66.66666666666667,
                tracker.getSuccessRate(),
                0.0001
        );
    }

    @Test
    void shouldReturnZeroSuccessRateInitially() {

        ProgressTracker tracker =
                new ProgressTracker();

        assertEquals(0.0, tracker.getSuccessRate());
    }
}