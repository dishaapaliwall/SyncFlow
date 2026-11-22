
package com.syncflow.snapshot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.TimeZone;
public class SnapshotStateManagerTest {

    public static void main(String[] args) throws Exception {

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));


        try (Connection connection = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5435/syncflow_target",
                "syncflow",
                "syncflow123")) {

            SnapshotStateManager manager =
                    new SnapshotStateManager(connection);

            manager.initialize();

            manager.beginBootstrap(
                    "0/1D400F0",
                    List.of(
                            "customers",
                            "products",
                            "orders",
                            "order_items",
                            "payments"
                    )
            );

            // Mark customers as currently copying
            manager.markTableCopying("customers");

// Simulate successful copy of 30 customer rows
            manager.markTableCopied("customers", 30);

// Display table-level progress
            try (var statement = connection.createStatement();
                 var rs = statement.executeQuery("""
         SELECT table_name, status, rows_copied
         FROM table_state
         ORDER BY table_name
         """)) {

                System.out.println("\nTable copy progress:");

                while (rs.next()) {
                    System.out.println(
                            rs.getString("table_name")
                                    + " | "
                                    + rs.getString("status")
                                    + " | rows: "
                                    + rs.getLong("rows_copied")
                    );
                }
            }

            // Test transition to CATCHING_UP
            manager.markCatchingUp();
            System.out.println(
                    "After catch-up transition: "
                            + manager.getBootstrapStatus());

// Test transition to STREAMING
            manager.markStreaming();
            System.out.println(
                    "After streaming transition: "
                            + manager.getBootstrapStatus());

// Clean up simulated test metadata
            try (var statement = connection.createStatement()) {

                statement.executeUpdate("DELETE FROM table_state");

                statement.executeUpdate("""
        UPDATE bootstrap_state
        SET status = 'NEW',
            snapshot_lsn = NULL,
            started_at = NULL,
            error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = 1
        """);
            }

            System.out.println(
                    "After cleanup: " + manager.getBootstrapStatus());

            System.out.println(
                    "Bootstrap status: " + manager.getBootstrapStatus());

            System.out.println(
                    "Registered tables: " + manager.getOwnedTables());

            System.out.println(
                    "Initial status: " + manager.getBootstrapStatus());

            System.out.println(
                    "Initial tables: " + manager.getOwnedTables());
        }
    }
}
