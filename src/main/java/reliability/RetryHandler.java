package reliability;

public class RetryHandler {

    private final int maxRetries;

    public RetryHandler(int maxRetries) {

        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    "Max retries cannot be negative"
            );
        }

        this.maxRetries = maxRetries;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public boolean shouldRetry(int attempt) {

        return attempt < maxRetries;
    }
}