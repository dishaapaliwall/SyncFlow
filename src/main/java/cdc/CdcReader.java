package cdc;

import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.SQLException;

public class CdcReader {

    private final PGReplicationStream replicationStream;

    public CdcReader(PGReplicationStream replicationStream) {
        this.replicationStream = replicationStream;
    }

    public ByteBuffer read() throws SQLException {
        return replicationStream.readPending();
    }
}