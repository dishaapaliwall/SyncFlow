package com.syncflow.event;

import com.syncflow.event.OperationType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OperationTypeTest {

    @Test
    void shouldContainAllOperations() {
        assertEquals(3, OperationType.values().length);

        assertTrue(OperationType.valueOf("INSERT") == OperationType.INSERT);
        assertTrue(OperationType.valueOf("UPDATE") == OperationType.UPDATE);
        assertTrue(OperationType.valueOf("DELETE") == OperationType.DELETE);
    }
}