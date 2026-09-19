package cdc;

import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CdcReaderTest {

    @Test
    void shouldReadPendingData() throws Exception {

        PGReplicationStream stream = mock(PGReplicationStream.class);

        ByteBuffer buffer =
                ByteBuffer.wrap("test-cdc-data".getBytes());

        when(stream.readPending()).thenReturn(buffer);

        CdcReader reader = new CdcReader(stream);

        ByteBuffer result = reader.read();

        assertNotNull(result);
        assertEquals(buffer, result);

        verify(stream).readPending();
    }

    @Test
    void shouldReturnNullWhenNoDataIsAvailable() throws Exception {

        PGReplicationStream stream = mock(PGReplicationStream.class);

        when(stream.readPending()).thenReturn(null);

        CdcReader reader = new CdcReader(stream);

        ByteBuffer result = reader.read();

        assertNull(result);

        verify(stream).readPending();
    }

    @Test
    void shouldAcknowledgeLastReceiveLsn() throws Exception {

        PGReplicationStream stream = mock(PGReplicationStream.class);
        LogSequenceNumber lsn = LogSequenceNumber.valueOf("0/16B2340");

        when(stream.getLastReceiveLSN()).thenReturn(lsn);

        CdcReader reader = new CdcReader(stream);
        reader.acknowledge();

        verify(stream).setFlushedLSN(lsn);
        verify(stream).setAppliedLSN(lsn);
        verify(stream).forceUpdateStatus();
    }
}