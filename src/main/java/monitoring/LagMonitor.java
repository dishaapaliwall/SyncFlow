package monitoring;

public class LagMonitor {

    private final long alertThresholdMillis;
    private long currentLagMillis;

    public LagMonitor(long alertThresholdMillis) {

        if (alertThresholdMillis < 0) {
            throw new IllegalArgumentException(
                    "Alert threshold cannot be negative"
            );
        }

        this.alertThresholdMillis = alertThresholdMillis;
    }

    public void updateLag(long lagMillis) {

        if (lagMillis < 0) {
            throw new IllegalArgumentException(
                    "Lag cannot be negative"
            );
        }

        currentLagMillis = lagMillis;
    }

    public long getCurrentLagMillis() {
        return currentLagMillis;
    }

    public boolean isAlertRequired() {
        return currentLagMillis > alertThresholdMillis;
    }

    public long getAlertThresholdMillis() {
        return alertThresholdMillis;
    }
}