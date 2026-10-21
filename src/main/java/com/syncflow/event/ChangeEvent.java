
package com.syncflow.event;

import java.time.LocalDateTime;
import java.util.Map;

public class ChangeEvent {

    private final String tableName;
    private final OperationType operation;
    private final Map<String, Object> data;
    private final Map<String, Object> oldData;
    private final String lsn;
    private final LocalDateTime timestamp;

    public ChangeEvent(
            String tableName,
            OperationType operation,
            Map<String, Object> data,
            String lsn,
            LocalDateTime timestamp
    ) {
        this(tableName, operation, data, null, lsn, timestamp);
    }

    public ChangeEvent(
            String tableName,
            OperationType operation,
            Map<String, Object> data,
            Map<String, Object> oldData,
            String lsn,
            LocalDateTime timestamp
    ) {
        this.tableName = tableName;
        this.operation = operation;
        this.data = data;
        this.oldData = oldData;
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

    public Map<String, Object> getOldData() {
        return oldData;
    }

    public String getLsn() {
        return lsn;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }
}
