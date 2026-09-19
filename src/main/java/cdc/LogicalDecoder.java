package cdc;

import event.ChangeEvent;
import event.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes PostgreSQL logical replication messages into ChangeEvent objects.
 */
public class LogicalDecoder {

    // Simple container to hold table name and its column names
    private static class TableSchema {
        String tableName;
        List<String> columns = new ArrayList<>();

        TableSchema(String tableName) {
            this.tableName = tableName;
        }
    }

    // Cache: Relation ID (e.g. 16384) -> TableSchema
    private final Map<Integer, TableSchema> schemaCache = new HashMap<>();

    private String currentLsn = "0/0";
    private LocalDateTime currentTimestamp = LocalDateTime.now();

    public ChangeEvent decode(ByteBuffer buffer) {

        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }

        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer cannot be empty");
        }

        // First byte tells the message type: B, R, I, U, D, C
        char messageType = (char) buffer.get();

        switch (messageType) {
            case 'B': // BEGIN
                parseBegin(buffer);
                return null;

            case 'R': // RELATION (Table schema metadata)
                parseRelation(buffer);
                return null;

            case 'I': // INSERT
                return parseInsert(buffer);

            case 'U': // UPDATE
                return parseUpdate(buffer);

            case 'D': // DELETE
                return parseDelete(buffer);

            case 'C': // COMMIT
                return null;

            default:
                // Ignore any other message types safely
                return null;
        }
    }

    // 1. Parse BEGIN: save LSN and current timestamp
    private void parseBegin(ByteBuffer buffer) {
        if (buffer.remaining() >= 8) {
            long lsn = buffer.getLong();
            this.currentLsn = Long.toHexString(lsn);
        }
        this.currentTimestamp = LocalDateTime.now();
    }

    // 2. Parse RELATION: read table name and columns, save to cache
    private void parseRelation(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        readCString(buffer); // Skip namespace (schema)
        String tableName = readCString(buffer);
        buffer.get(); // Skip replica identity byte

        short columnCount = buffer.getShort();
        TableSchema schema = new TableSchema(tableName);

        for (int i = 0; i < columnCount; i++) {
            buffer.get(); // Skip column flags
            String columnName = readCString(buffer);
            buffer.getInt(); // Skip type OID
            buffer.getInt(); // Skip type modifier
            schema.columns.add(columnName);
        }

        schemaCache.put(relationId, schema);
    }

    // 3. Parse INSERT: read new row values and create ChangeEvent
    private ChangeEvent parseInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = schemaCache.get(relationId);
        if (schema == null) {
            return null;
        }

        buffer.get(); // Skip 'N' (New tuple indicator)
        Map<String, Object> rowData = readTuple(buffer, schema.columns);

        return new ChangeEvent(
                schema.tableName,
                OperationType.INSERT,
                rowData,
                currentLsn,
                currentTimestamp
        );
    }

    // 4. Parse UPDATE: read updated row values and create ChangeEvent
    private ChangeEvent parseUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = schemaCache.get(relationId);
        if (schema == null) {
            return null;
        }

        char tupleType = (char) buffer.get();
        // If old key ('K') or old tuple ('O') exists, skip it first
        if (tupleType == 'K' || tupleType == 'O') {
            readTuple(buffer, schema.columns);
            buffer.get(); // Skip 'N' marker
        }

        Map<String, Object> rowData = readTuple(buffer, schema.columns);

        return new ChangeEvent(
                schema.tableName,
                OperationType.UPDATE,
                rowData,
                currentLsn,
                currentTimestamp
        );
    }

    // 5. Parse DELETE: read deleted row key and create ChangeEvent
    private ChangeEvent parseDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = schemaCache.get(relationId);
        if (schema == null) {
            return null;
        }

        buffer.get(); // Skip 'K' or 'O' marker
        Map<String, Object> rowData = readTuple(buffer, schema.columns);

        return new ChangeEvent(
                schema.tableName,
                OperationType.DELETE,
                rowData,
                currentLsn,
                currentTimestamp
        );
    }

    // Helper to read column values of a row
    private Map<String, Object> readTuple(ByteBuffer buffer, List<String> columns) {
        short columnCount = buffer.getShort();
        Map<String, Object> row = new HashMap<>();

        for (int i = 0; i < columnCount; i++) {
            char valueType = (char) buffer.get();
            String colName = (i < columns.size()) ? columns.get(i) : ("col_" + i);

            if (valueType == 'n') {
                // 'n' means value is NULL
                row.put(colName, null);
            } else if (valueType == 't') {
                // 't' means value is text
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                row.put(colName, new String(bytes, StandardCharsets.UTF_8));
            }
        }
        return row;
    }

    // Helper to read a null-terminated string (C-string)
    private String readCString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }
}