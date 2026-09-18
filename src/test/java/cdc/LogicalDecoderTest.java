package cdc;

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

        ByteBuffer buffer =
                ByteBuffer.wrap("test-cdc-data".getBytes());

        assertDoesNotThrow(
                () -> decoder.decode(buffer)
        );
    }
}