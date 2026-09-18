package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.TimeZone;

public class DatabaseConnectionTest {

    public static void main(String[] args) {

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));

        String sourceUrl = "jdbc:postgresql://localhost:5434/syncflow_source";
        String targetUrl = "jdbc:postgresql://localhost:5435/syncflow_target";

        String username = "syncflow";
        String password = "syncflow123";

        try {
            Connection sourceConnection =
                    DriverManager.getConnection(sourceUrl, username, password);

            System.out.println("Source Database Connected!");

            Connection targetConnection =
                    DriverManager.getConnection(targetUrl, username, password);

            System.out.println("Target Database Connected!");

            sourceConnection.close();
            targetConnection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}