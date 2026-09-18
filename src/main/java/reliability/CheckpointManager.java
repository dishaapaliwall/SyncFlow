package reliability;

public class CheckpointManager {

    private String lastProcessedLsn;

    public void saveCheckpoint(String lsn) {

        if (lsn == null || lsn.isBlank()) {
            throw new IllegalArgumentException("LSN cannot be empty");
        }

        lastProcessedLsn = lsn;
    }

    public String getLastProcessedLsn() {
        return lastProcessedLsn;
    }
}