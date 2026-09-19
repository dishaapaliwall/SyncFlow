package cdc;

import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Reads streaming WAL events from PostgreSQL logical replication stream
 * and sends acknowledgment feedback back to PostgreSQL.
 */
public class CdcReader {

    private final PGReplicationStream replicationStream;

    public CdcReader(PGReplicationStream replicationStream) {
        this.replicationStream = replicationStream;
    }

    /**
     * Reads pending data from the replication stream.
     * Returns null if no data is currently waiting.
     */
    public ByteBuffer read() throws SQLException {
        return replicationStream.readPending();
    }

    /**
     * Gets the latest LSN received from the replication stream.
     */
    public LogSequenceNumber getLastReceiveLSN() {
        return replicationStream.getLastReceiveLSN();
    }

    /**
     * Sends acknowledgment back to PostgreSQL for a specific LSN.
     * This allows PostgreSQL to safely prune processed WAL logs.
     */
    public void acknowledge(LogSequenceNumber lsn) throws SQLException {
        if (lsn != null) {
            replicationStream.setFlushedLSN(lsn);
            replicationStream.setAppliedLSN(lsn);
            replicationStream.forceUpdateStatus();
        }
    }

    /**
     * Sends acknowledgment for the most recently received LSN.
     */
    public void acknowledge() throws SQLException {
        acknowledge(replicationStream.getLastReceiveLSN());
    }

    /**
     * Closes the underlying replication stream.
     */
    public void close() throws SQLException {
        if (replicationStream != null && !replicationStream.isClosed()) {
            replicationStream.close();
        }
    }
}