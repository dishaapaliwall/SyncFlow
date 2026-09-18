package monitoring;

public class ProgressTracker {

    private long totalEvents;
    private long successfulEvents;
    private long failedEvents;

    public void recordSuccess() {
        totalEvents++;
        successfulEvents++;
    }

    public void recordFailure() {
        totalEvents++;
        failedEvents++;
    }

    public long getTotalEvents() {
        return totalEvents;
    }

    public long getSuccessfulEvents() {
        return successfulEvents;
    }

    public long getFailedEvents() {
        return failedEvents;
    }

    public double getSuccessRate() {

        if (totalEvents == 0) {
            return 0.0;
        }

        return (successfulEvents * 100.0) / totalEvents;
    }
}