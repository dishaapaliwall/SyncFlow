package com.syncflow.controller;

import com.syncflow.service.SyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SyncControllerTest {

    private SyncService syncService;
    private SyncController syncController;

    @BeforeEach
    void setUp() {
        syncService = Mockito.mock(SyncService.class);
        syncController = new SyncController(syncService);
    }

    @Test
    void shouldInitiateBootstrapWhenNotRunning() {
        when(syncService.isRunning()).thenReturn(false);

        ResponseEntity<Map<String, Object>> response = syncController.bootstrapSync(null);

        assertEquals(200, response.getStatusCode().value());
        assertEquals("STARTED", response.getBody().get("status"));
    }

    @Test
    void shouldRejectBootstrapWhenAlreadyRunning() {
        when(syncService.isRunning()).thenReturn(true);

        ResponseEntity<Map<String, Object>> response = syncController.bootstrapSync(null);

        assertEquals(400, response.getStatusCode().value());
        assertEquals("ERROR", response.getBody().get("status"));
    }
}