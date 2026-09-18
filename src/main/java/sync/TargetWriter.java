package sync;

import event.ChangeEvent;
import event.OperationType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class TargetWriter {

    private final Connection connection;

    public TargetWriter(Connection connection) {
        this.connection = connection;
    }

    public void write(ChangeEvent event) throws SQLException {

        if (event.getOperation() == OperationType.INSERT) {
            insert(event);
        }
    }

    private void insert(ChangeEvent event) throws SQLException {

        String tableName = event.getTableName();
        Map<String, Object> data = event.getData();

        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Event data cannot be empty");
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for (String column : data.keySet()) {

            if (columns.length() > 0) {
                columns.append(", ");
                values.append(", ");
            }

            columns.append(column);
            values.append("?");
        }

        String sql = "INSERT INTO " + tableName +
                " (" + columns + ") VALUES (" + values + ")";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {

            int index = 1;

            for (Object value : data.values()) {
                statement.setObject(index++, value);
            }

            statement.executeUpdate();
        }
    }
}