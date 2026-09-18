package sync;

import event.ChangeEvent;

import java.sql.SQLException;
import java.util.List;

public class BatchProcessor {

    private static final int BATCH_SIZE = 10;

    private final TargetWriter targetWriter;
    private final TransactionManager transactionManager;

    public BatchProcessor(
            TargetWriter targetWriter,
            TransactionManager transactionManager
    ) {
        this.targetWriter = targetWriter;
        this.transactionManager = transactionManager;
    }

    public void process(List<ChangeEvent> events) throws SQLException {

        if (events == null || events.isEmpty()) {
            return;
        }

        if (events.size() > BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch cannot contain more than 10 events"
            );
        }

        transactionManager.begin();

        try {
            for (ChangeEvent event : events) {
                targetWriter.write(event);
            }

            transactionManager.commit();

        } catch (Exception e) {
            transactionManager.rollback();
            throw e;
        }
    }
}