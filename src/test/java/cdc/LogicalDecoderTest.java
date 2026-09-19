package cdc;

import event.ChangeEvent;
import event.OperationType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class LogicalDecoderTest {

    @Test
    void shouldRejectNullBuffer() {
        LogicalDecoder decoder = new LogicalDecoder();

        assertThrows(
                IllegalArgumentException.class,
                () -> decoder.decode(null)
        );
    }

    @Test
    void shouldRejectEmptyBuffer() {
        LogicalDecoder decoder = new LogicalDecoder();
        ByteBuffer buffer = ByteBuffer.allocate(0);

        assertThrows(
                IllegalArgumentException.class,
                () -> decoder.decode(buffer)
        );
    }

    @Test
    void shouldAcceptNonEmptyBuffer() {
        LogicalDecoder decoder = new LogicalDecoder();
        ByteBuffer buffer = ByteBuffer.wrap("test-cdc-data".getBytes());

        assertDoesNotThrow(
                () -> decoder.decode(buffer)
        );
    }

    @Test
    void shouldDecodeInsertEventSuccessfully() {
        LogicalDecoder decoder = new LogicalDecoder();

        // 1. Simulate 'R' (Relation) message: table "customers", column "name"
        ByteBuffer relationBuffer = ByteBuffer.allocate(64);
        relationBuffer.put((byte) 'R');
        relationBuffer.putInt(1001); // relation ID
        relationBuffer.put("public".getBytes()); // namespace
        relationBuffer.put((byte) 0); // null terminator for namespace
        relationBuffer.put("customers".getBytes()); // table name
        relationBuffer.put((byte) 0); // null terminator for table name
        relationBuffer.put((byte) 'd'); // replica identity
        relationBuffer.putShort((short) 1); // 1 column
        relationBuffer.put((byte) 0); // column flag
        relationBuffer.put("name".getBytes()); // column name
        relationBuffer.put((byte) 0); // null terminator for column name
        relationBuffer.putInt(25); // type OID
        relationBuffer.putInt(-1); // type modifier
        relationBuffer.flip();

        ChangeEvent relationResult = decoder.decode(relationBuffer);
        assertNull(relationResult); // Relation message is metadata, so returns null

        // 2. Simulate 'I' (Insert) message: relationId 1001, new tuple 'N', value "Alice"
        ByteBuffer insertBuffer = ByteBuffer.allocate(64);
        insertBuffer.put((byte) 'I');
        insertBuffer.putInt(1001); // relation ID
        insertBuffer.put((byte) 'N'); // New tuple indicator
        insertBuffer.putShort((short) 1); // 1 column
        insertBuffer.put((byte) 't'); // text value indicator
        byte[] textBytes = "Alice".getBytes();
        insertBuffer.putInt(textBytes.length);
        insertBuffer.put(textBytes);
        insertBuffer.flip();

        ChangeEvent event = decoder.decode(insertBuffer);

        // 3. Verify the decoded ChangeEvent
        assertNotNull(event);
        assertEquals("customers", event.getTableName());
        assertEquals(OperationType.INSERT, event.getOperation());
        assertEquals("Alice", event.getData().get("name"));
    }
}