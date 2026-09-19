package cdc;

import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ReplicationSlotTest {

    @Test
    void shouldCreateReplicationSlot() throws Exception {

        CdcConnection cdcConnection =
                new CdcConnection(
                        "jdbc:postgresql://localhost:5434/syncflow_source",
                        "syncflow",
                        "syncflow123"
                );

        cdcConnection.connect();

        PGConnection pgConnection =
                cdcConnection.getPGConnection();

        assertNotNull(pgConnection);

        try {
            pgConnection
                    .getReplicationAPI()
                    .createReplicationSlot()
                    .logical()
                    .withSlotName("syncflow_slot")
                    .withOutputPlugin("pgoutput")
                    .make();

            System.out.println("Replication slot created successfully!");
        } catch (org.postgresql.util.PSQLException e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                System.out.println("Replication slot already exists, continuing!");
            } else {
                throw e;
            }
        }

        cdcConnection.close();
    }
}