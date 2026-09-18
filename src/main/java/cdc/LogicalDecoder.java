package cdc;

import event.ChangeEvent;

import java.nio.ByteBuffer;

public class LogicalDecoder {

    public ChangeEvent decode(ByteBuffer buffer) {

        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }

        // Actual PostgreSQL WAL decoding will be implemented
        // when the replication stream is connected.
        // For now, this class validates that CDC data is received.

        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer cannot be empty");
        }

        return null;
    }
}