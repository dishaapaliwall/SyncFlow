
package com.syncflow.cdc;

import com.syncflow.event.ChangeEvent;
import com.syncflow.event.OperationType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import java.time.Instant;
import java.time.ZoneOffset;

public class LogicalDecoder {

    private static class TableSchema {
        String tableName;
        String schemaName;

        List<String> columns = new ArrayList<>();
        List<Boolean> keyColumns = new ArrayList<>();
        List<Integer> typeOids = new ArrayList<>();

        TableSchema(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }
    }

    private final Map<Integer, TableSchema> schemaCache = new HashMap<>();

    private String currentLsn = "0/0";
    private LocalDateTime currentTimestamp = LocalDateTime.now();
    private boolean commitMessage;

    private String commitLsn;
    private String endLsn;

    public ChangeEvent decode(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }

        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer cannot be empty");
        }

        commitMessage = false;
        char type = (char) buffer.get();

        try {
            switch (type) {
                case 'B':
                    parseBegin(buffer);
                    return null;

                case 'R':
                    parseRelation(buffer);
                    return null;

                case 'I':
                    return parseInsert(buffer);

                case 'U':
                    return parseUpdate(buffer);

                case 'D':
                    return parseDelete(buffer);

                case 'C':
                    parseCommit(buffer);
                    commitMessage = true;
                    return null;

                default:
                    return null;
            }
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException(
                    "Incomplete PostgreSQL replication message: " + type, e);
        }
    }

    private void parseCommit(ByteBuffer buffer) {

        // Flags (currently reserved by pgoutput)
        buffer.get();

        // LSN where the commit record starts
        long commitLsnValue = buffer.getLong();

        // LSN immediately after the transaction commit record
        long endLsnValue = buffer.getLong();

        // PostgreSQL timestamp: microseconds since 2000-01-01 UTC
        long commitTimeMicros = buffer.getLong();

        this.commitLsn = formatLsn(commitLsnValue);
        this.endLsn = formatLsn(endLsnValue);

        this.currentLsn = this.commitLsn;
        this.currentTimestamp = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(946684800L)
                        .plusNanos(commitTimeMicros * 1000L),
                ZoneOffset.UTC
        );
    }


    private String formatLsn(long lsn) {

        long high = (lsn >>> 32) & 0xFFFFFFFFL;
        long low = lsn & 0xFFFFFFFFL;

        return String.format("%X/%X", high, low);
    }



    private void parseBegin(ByteBuffer buffer) {

        // LSN of the commit record
        long finalLsn = buffer.getLong();

        // Source transaction commit timestamp
        long commitTimeMicros = buffer.getLong();

        // Transaction ID (currently not needed)
        buffer.getInt();

        this.currentLsn = formatLsn(finalLsn);

        this.currentTimestamp = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(946684800L)
                        .plusNanos(commitTimeMicros * 1000L),
                ZoneOffset.UTC
        );

        this.commitLsn = null;
        this.endLsn = null;
    }

    private void parseRelation(ByteBuffer buffer) {
        int relationId = buffer.getInt();

        String schemaName = readCString(buffer);
        String tableName = readCString(buffer);

        buffer.get(); // Replica identity

        int count = Short.toUnsignedInt(buffer.getShort());
        TableSchema schema = new TableSchema(schemaName, tableName);

        for (int i = 0; i < count; i++) {
            int flags = Byte.toUnsignedInt(buffer.get());
            String columnName = readCString(buffer);
            int typeOid = buffer.getInt();
            buffer.getInt(); // Type modifier

            schema.columns.add(columnName);
            schema.keyColumns.add((flags & 1) != 0);
            schema.typeOids.add(typeOid);
        }

        schemaCache.put(relationId, schema);

        System.out.println(
                "[RELATION] table=" + schema.tableName
                        + ", columns=" + schema.columns
                        + ", keyColumns=" + schema.keyColumns
        );
    }

    private ChangeEvent parseInsert(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = getSchema(relationId);

        if (schema == null) {
            return null;
        }

        requireMarker(buffer, 'N');

        Map<String, Object> data = readTuple(buffer, schema, false);

        return new ChangeEvent(
                schema.tableName,
                OperationType.INSERT,
                data,
                null,
                currentLsn,
                currentTimestamp
        );
    }

    private ChangeEvent parseUpdate(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = getSchema(relationId);

        if (schema == null) {
            return null;
        }

        Map<String, Object> oldData = null;
        char marker = (char) buffer.get();

        System.out.println(
                "[UPDATE DEBUG] table=" + schema.tableName
                        + ", marker=" + marker
        );

        if (marker == 'K' || marker == 'O') {
            Map<String, Object> previousRow =
                    readTuple(buffer, schema, marker == 'K');

            oldData = marker == 'K'
                    ? previousRow
                    : extractKey(previousRow, schema);

            requireMarker(buffer, 'N');

        } else if (marker != 'N') {
            throw new IllegalArgumentException(
                    "Unexpected UPDATE tuple marker: " + marker
            );
        }

        // The N marker has already been consumed.
        Map<String, Object> newData = readTuple(buffer, schema, false);

        // When PostgreSQL supplies no old tuple, use the primary key
        // from the new tuple, provided that the primary key is unchanged.
        if (oldData == null || oldData.isEmpty()) {
            oldData = extractKey(newData, schema);
        }

        System.out.println("[UPDATE DEBUG] oldData=" + oldData);
        System.out.println("[UPDATE DEBUG] newData=" + newData);

        if (oldData == null || oldData.isEmpty()) {
            throw new IllegalArgumentException(
                    "UPDATE requires primary-key values for table: "
                            + schema.tableName
            );
        }

        return new ChangeEvent(
                schema.tableName,
                OperationType.UPDATE,
                newData,
                oldData,
                currentLsn,
                currentTimestamp
        );
    }

    private ChangeEvent parseDelete(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        TableSchema schema = getSchema(relationId);

        if (schema == null) {
            return null;
        }

        char marker = (char) buffer.get();

        if (marker != 'K' && marker != 'O') {
            throw new IllegalArgumentException(
                    "Unexpected DELETE tuple marker: " + marker
            );
        }

        Map<String, Object> previousRow =
                readTuple(buffer, schema, marker == 'K');

        Map<String, Object> oldData = marker == 'K'
                ? previousRow
                : extractKey(previousRow, schema);

        if (oldData == null || oldData.isEmpty()) {
            throw new IllegalArgumentException(
                    "DELETE requires old primary-key values for table: "
                            + schema.tableName
            );
        }

        return new ChangeEvent(
                schema.tableName,
                OperationType.DELETE,
                oldData,
                oldData,
                currentLsn,
                currentTimestamp
        );
    }

    private TableSchema getSchema(int relationId) {
        return schemaCache.get(relationId);
    }

    private Map<String, Object> readTuple(
            ByteBuffer buffer,
            TableSchema schema,
            boolean keyTuple) {

        int count = Short.toUnsignedInt(buffer.getShort());

        if (count > schema.columns.size()) {
            throw new IllegalArgumentException(
                    "Tuple has " + count
                            + " columns, but relation "
                            + schema.tableName + " has only "
                            + schema.columns.size() + " columns"
            );
        }

        Map<String, Object> row = new LinkedHashMap<>();

        for (int i = 0; i < count; i++) {

            String columnName = schema.columns.get(i);
            boolean isKeyColumn = schema.keyColumns.get(i);

            char valueType = (char) buffer.get();

            if (valueType == 'n') {
                if (!keyTuple || isKeyColumn) {
                    row.put(columnName, null);
                }

            } else if (valueType == 't') {

                int length = buffer.getInt();

                if (length < 0 || length > buffer.remaining()) {
                    throw new IllegalArgumentException(
                            "Invalid tuple value length for column: "
                                    + columnName
                    );
                }

                byte[] bytes = new byte[length];
                buffer.get(bytes);

                String value = new String(
                        bytes,
                        StandardCharsets.UTF_8
                );

                int oid = schema.typeOids.get(i);

                // Key tuples only need to retain key-column values.
                if (!keyTuple || isKeyColumn) {
                    row.put(columnName, convertValue(value, oid));
                }

            } else if (valueType == 'u') {

                // Unchanged TOAST value.
                // Do not overwrite the existing value with NULL.

            } else {
                throw new IllegalArgumentException(
                        "Unsupported tuple value marker: "
                                + (int) valueType
                                + " ('" + valueType + "')"
                                + ", column=" + columnName
                                + ", table=" + schema.tableName
                                + ", tupleColumnIndex=" + i
                );
            }
        }

        return row;
    }

    private Map<String, Object> extractKey(
            Map<String, Object> row,
            TableSchema schema) {

        if (row == null || row.isEmpty()) {
            return null;
        }

        Map<String, Object> key = new LinkedHashMap<>();

        for (int i = 0; i < schema.columns.size(); i++) {
            if (schema.keyColumns.get(i)) {
                String column = schema.columns.get(i);

                if (row.containsKey(column)) {
                    key.put(column, row.get(column));
                }
            }
        }

        return key.isEmpty() ? null : key;
    }

    private Object convertValue(String value, int oid) {
        return switch (oid) {
            case 16 -> value.equals("t");
            case 20 -> Long.valueOf(value);
            case 21 -> Short.valueOf(value);
            case 23 -> Integer.valueOf(value);
            case 700 -> Float.valueOf(value);
            case 701 -> Double.valueOf(value);
            case 1700 -> new java.math.BigDecimal(value);
            case 1082 -> java.sql.Date.valueOf(value);
            case 1114 -> java.sql.Timestamp.valueOf(value);
            case 1184 -> java.time.OffsetDateTime.parse(
                    value.replace(" ", "T"));
            case 2950 -> java.util.UUID.fromString(value);
            default -> value;
        };
    }

    private void requireMarker(ByteBuffer buffer, char expected) {
        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException(
                    "Missing tuple marker: " + expected
            );
        }

        char actual = (char) buffer.get();

        if (actual != expected) {
            throw new IllegalArgumentException(
                    "Expected tuple marker " + expected + " but got " + actual
            );
        }
    }

    private String readCString(ByteBuffer buffer) {
        StringBuilder result = new StringBuilder();

        while (buffer.hasRemaining()) {
            byte b = buffer.get();

            if (b == 0) {
                return result.toString();
            }

            result.append((char) (b & 0xff));
        }

        throw new IllegalArgumentException(
                "Unterminated PostgreSQL C-string"
        );
    }


    public String getCommitLsn() {
        return commitLsn;
    }

    public String getEndLsn() {
        return endLsn;
    }

    public LocalDateTime getCommitTimestamp() {
        return currentTimestamp;
    }

    public boolean isCommitMessage() {
        return commitMessage;
    }
}
