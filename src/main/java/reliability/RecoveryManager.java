package reliability;

public class RecoveryManager {

    private final CheckpointManager checkpointManager;

    public RecoveryManager(CheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    public String recover() {

        return checkpointManager.getLastProcessedLsn();
    }

    public boolean canRecover() {

        return checkpointManager.getLastProcessedLsn() != null;
    }
}