
package com.syncflow.snapshot;

import java.sql.*;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.StringJoiner;

public class SnapshotWorker {

    private static final Pattern VALID_IDENTIFIER =
            Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    private static final int BATCH_SIZE = 500;

    private final String sourceUrl;
    private final String targetUrl;
    private final String username;
    private final String password;
    private final SnapshotStateManager stateManager;

    public SnapshotWorker(
            String sourceUrl,
            String targetUrl,
            String username,
            String password,
            SnapshotStateManager stateManager) {

        this.sourceUrl = sourceUrl;
        this.targetUrl = targetUrl;
        this.username = username;
        this.password = password;
        this.stateManager = Objects.requireNonNull(
                stateManager, "SnapshotStateManager cannot be null");
    }

    /**
     * Imports the exported snapshot and copies one source table
     * into its corresponding target table.
     *
     * The exporter connection must remain open until this worker
     * has imported the snapshot.
     *
     * @return number of rows copied
     */
    public long copyTable(
            String snapshotName,
            String sourceTable,
            String targetTable) throws SQLException {

        validateIdentifier(sourceTable);
        validateIdentifier(targetTable);

        if (snapshotName == null
                || !snapshotName.matches("[A-Za-z0-9_-]+")) {
            throw new IllegalArgumentException(
                    "Invalid exported snapshot name");
        }

        stateManager.markTableCopying(sourceTable);

        long rowsCopied = 0;

        try (Connection source = DriverManager.getConnection(
                sourceUrl, username, password);
             Connection target = DriverManager.getConnection(
                     targetUrl, username, password)) {

            // Import the exported snapshot before executing any query.
            source.setTransactionIsolation(
                    Connection.TRANSACTION_REPEATABLE_READ);

            source.setAutoCommit(false);

            try (Statement snapshotStatement =
                         source.createStatement()) {

                snapshotStatement.execute(
                        "SET TRANSACTION SNAPSHOT '"
                                + snapshotName + "'");
            }

            System.out.println(
                    "[WORKER] Imported snapshot: " + snapshotName);

            // Start the target transaction.
            target.setAutoCommit(false);

            try {

                String selectSql =
                        "SELECT * FROM public."
                                + quoteIdentifier(sourceTable);

                try (Statement selectStatement =
                             source.createStatement();
                     ResultSet resultSet =
                             selectStatement.executeQuery(selectSql)) {

                    ResultSetMetaData metadata =
                            resultSet.getMetaData();

                    int columnCount = metadata.getColumnCount();

                    StringJoiner columns = new StringJoiner(", ");
                    StringJoiner placeholders = new StringJoiner(", ");

                    for (int i = 1; i <= columnCount; i++) {
                        columns.add(
                                quoteIdentifier(
                                        metadata.getColumnName(i)));

                        placeholders.add("?");
                    }

                    String insertSql =
                            "INSERT INTO public."
                                    + quoteIdentifier(targetTable)
                                    + " (" + columns + ") VALUES ("
                                    + placeholders + ")";

                    try (PreparedStatement insertStatement =
                                 target.prepareStatement(insertSql)) {

                        int pendingBatch = 0;

                        while (resultSet.next()) {

                            for (int i = 1;
                                 i <= columnCount;
                                 i++) {

                                insertStatement.setObject(
                                        i, resultSet.getObject(i));
                            }

                            insertStatement.addBatch();

                            rowsCopied++;
                            pendingBatch++;

                            if (pendingBatch == BATCH_SIZE) {
                                insertStatement.executeBatch();
                                insertStatement.clearBatch();
                                pendingBatch = 0;
                            }
                        }

                        if (pendingBatch > 0) {
                            insertStatement.executeBatch();
                            insertStatement.clearBatch();
                        }
                    }
                }

                // All rows for this table commit together.
                target.commit();

            } catch (SQLException | RuntimeException e) {

                try {
                    target.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }

                throw e;
            }

            // The source snapshot transaction is no longer needed.
            source.rollback();

        } catch (SQLException | RuntimeException e) {

            try {
                stateManager.markTableFailed(sourceTable);
            } catch (SQLException stateError) {
                e.addSuppressed(stateError);
            }

            throw e;
        }

        // Record progress after the target transaction commits.
        stateManager.markTableCopied(sourceTable, rowsCopied);

        System.out.println(
                "[WORKER] Table copied: " + sourceTable);

        System.out.println(
                "[WORKER] Rows copied: " + rowsCopied);

        return rowsCopied;
    }

    private void validateIdentifier(String identifier) {

        if (identifier == null
                || !VALID_IDENTIFIER.matcher(identifier).matches()) {

            throw new IllegalArgumentException(
                    "Invalid SQL identifier: " + identifier);
        }
    }

    private String quoteIdentifier(String identifier) {
        validateIdentifier(identifier);
        return "\"" + identifier + "\"";
    }
}
