package cdc;

import org.junit.jupiter.api.Test;
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
}