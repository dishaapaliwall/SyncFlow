package cdc;

import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class CdcConnection {

    private final String url;
    private final String username;
    private final String password;

    private Connection connection;

    public CdcConnection(
            String url,
            String username,
            String password
    ) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection connect() throws SQLException {

        TimeZone.setDefault(
                TimeZone.getTimeZone("Asia/Kolkata")
        );

        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);

        // PostgreSQL logical replication settings
        properties.setProperty("replication", "database");
        properties.setProperty("assumeMinServerVersion", "9.4");
        properties.setProperty("preferQueryMode", "simple");

        connection = DriverManager.getConnection(url, properties);

        return connection;
    }

    public PGConnection getPGConnection() throws SQLException {

        if (connection == null || connection.isClosed()) {
            throw new SQLException(
                    "CDC connection is not established"
            );
        }

        return connection.unwrap(PGConnection.class);
    }

    public PGReplicationStream createReplicationStream(
            String slotName,
            String publicationName
    ) throws SQLException {

        if (connection == null || connection.isClosed()) {
            throw new SQLException(
                    "CDC connection is not established"
            );
        }

        PGConnection pgConnection =
                connection.unwrap(PGConnection.class);

        return pgConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", "1")
                .withSlotOption(
                        "publication_names",
                        publicationName
                )
                .start();
    }

    public void close() throws SQLException {

        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}