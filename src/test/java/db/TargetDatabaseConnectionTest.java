package db;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TargetDatabaseConnectionTest {

    @Test
    void shouldConnectToTargetDatabase() throws Exception {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata")
        );

        String url = "jdbc:postgresql://localhost:5435/syncflow_target";
        String username = "syncflow";
        String password = "syncflow123";

        Connection connection =
                DriverManager.getConnection(url, username, password);

        assertNotNull(connection);

        System.out.println("Connected to Target PostgreSQL successfully!");

        connection.close();
    }
}