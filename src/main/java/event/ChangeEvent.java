package event;

import java.time.LocalDateTime;
import java.util.Map;

public class ChangeEvent {

    private final String tableName;
    private final OperationType operation;
    private final Map<String, Object> data;
    private final String lsn;
    private final LocalDateTime timestamp;

    public ChangeEvent(
            String tableName,
            OperationType operation,
            Map<String, Object> data,
            String lsn,
            LocalDateTime timestamp
    ) {
        this.tableName = tableName;
        this.operation = operation;
        this.data = data;
        this.lsn = lsn;
        this.timestamp = timestamp;
    }

    public String getTableName() {
        return tableName;
    }

    public OperationType getOperation() {
        return operation;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public String getLsn() {
        return lsn;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }
}