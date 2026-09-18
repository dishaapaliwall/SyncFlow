package cdc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

class CdcConnectionTest {

    @Test
    void shouldConnectToSourceDatabase() throws Exception {

        CdcConnection cdcConnection = new CdcConnection(
                "jdbc:postgresql://localhost:5434/syncflow_source",
                "syncflow",
                "syncflow123"
        );

        Connection connection = cdcConnection.connect();

        assertNotNull(connection);
        assertFalse(connection.isClosed());

        cdcConnection.close();
    }
}