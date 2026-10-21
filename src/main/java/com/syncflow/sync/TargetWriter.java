
package com.syncflow.sync;

import com.syncflow.event.ChangeEvent;
import com.syncflow.event.OperationType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public class TargetWriter {

    private static final Pattern VALID_IDENTIFIER =
            Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private final Connection connection;

    public TargetWriter(Connection connection) {
        this.connection = connection;
    }

    public void write(ChangeEvent event) throws SQLException {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }

        switch (event.getOperation()) {
            case INSERT:
                insert(event);
                break;
            case UPDATE:
                update(event);
                break;
            case DELETE:
                delete(event);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported operation: " + event.getOperation());
        }
    }

    private void insert(ChangeEvent event) throws SQLException {
        Map<String, Object> data = event.getData();
        validateData(data);

        StringJoiner columns = new StringJoiner(", ");
        StringJoiner placeholders = new StringJoiner(", ");

        for (String column : data.keySet()) {
            columns.add(identifier(column));
            placeholders.add("?");
        }

        String sql = "INSERT INTO " + identifier(event.getTableName())
                + " (" + columns + ") VALUES (" + placeholders + ")";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : data.values()) {
                statement.setObject(index++, value);
            }
            statement.executeUpdate();
        }
    }

    private void update(ChangeEvent event) throws SQLException {
        Map<String, Object> newData = event.getData();
        Map<String, Object> oldData = event.getOldData();

        validateData(newData);
        validateData(oldData);

        StringJoiner assignments = new StringJoiner(", ");
        for (String column : newData.keySet()) {
            assignments.add(identifier(column) + " = ?");
        }

        StringJoiner conditions = new StringJoiner(" AND ");
        for (String column : oldData.keySet()) {
            conditions.add(identifier(column) + " IS NOT DISTINCT FROM ?");
        }

        String sql = "UPDATE " + identifier(event.getTableName())
                + " SET " + assignments
                + " WHERE " + conditions;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;

            for (Object value : newData.values()) {
                statement.setObject(index++, value);
            }

            for (Object value : oldData.values()) {
                statement.setObject(index++, value);
            }

            int updated = statement.executeUpdate();

            if (updated != 1) {
                throw new SQLException(
                        "Expected to update one row in " + event.getTableName()
                                + " but updated " + updated);
            }
        }
    }

    private void delete(ChangeEvent event) throws SQLException {
        Map<String, Object> oldData = event.getOldData();
        if (oldData == null || oldData.isEmpty()) {
            oldData = event.getData();
        }

        validateData(oldData);

        StringJoiner conditions = new StringJoiner(" AND ");
        for (String column : oldData.keySet()) {
            conditions.add(identifier(column) + " IS NOT DISTINCT FROM ?");
        }

        String sql = "DELETE FROM " + identifier(event.getTableName())
                + " WHERE " + conditions;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;

            for (Object value : oldData.values()) {
                statement.setObject(index++, value);
            }

            int deleted = statement.executeUpdate();

            if (deleted != 1) {
                throw new SQLException(
                        "Expected to delete one row from " + event.getTableName()
                                + " but deleted " + deleted);
            }
        }
    }

    private void validateData(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException(
                    "Row data cannot be null or empty");
        }

        for (String column : data.keySet()) {
            identifier(column);
        }
    }

    private String identifier(String value) {
        if (value == null || !VALID_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid SQL identifier: " + value);
        }
        return "\"" + value + "\"";
    }
}
