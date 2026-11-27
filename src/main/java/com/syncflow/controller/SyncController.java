package com.syncflow.controller;

import com.syncflow.event.ChangeEvent;
import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import com.syncflow.service.SyncService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/sync")
public class SyncController {

    private final SyncService syncService;

    // Default tables in foreign-key dependency order
    private static final Map<String, String> DEFAULT_TABLES = new LinkedHashMap<>();
    static {
        DEFAULT_TABLES.put("customers", "customers");
        DEFAULT_TABLES.put("products", "products");
        DEFAULT_TABLES.put("orders", "orders");
        DEFAULT_TABLES.put("order_items", "order_items");
        DEFAULT_TABLES.put("payments", "payments");
    }

    public SyncController(SyncService syncService) {
        this.syncService = syncService;
    }

    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        return Map.of(
                "service", "SyncFlow",
                "status", syncService.isRunning() ? "RUNNING" : "STOPPED",
                "connectionStatus",
                syncService.isConnected() ? "CONNECTED" : "DISCONNECTED"
        );
    }

    /**
     * Start raw CDC streaming only (does not copy old data).
     */
    @PostMapping("/start")
    public Map<String, String> startSync() {
        if (syncService.isRunning()) {
            return Map.of("message", "SyncFlow is already running");
        }

        syncService.start();
        return Map.of("message", "CDC engine start requested");
    }

    /**
     * Initial Snapshot + CDC:
     * Copies all existing table rows from source to target first,
     * then seamlessly hands off to continuous CDC streaming.
     */
    @PostMapping("/bootstrap")
    public ResponseEntity<Map<String, Object>> bootstrapSync(
            @RequestBody(required = false) Map<String, String> tables) {

        if (syncService.isRunning()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "SyncFlow is already running. Stop CDC before running bootstrap."
            ));
        }

        Map<String, String> targetTables = (tables != null && !tables.isEmpty())
                ? tables
                : DEFAULT_TABLES;

        // Run in background so HTTP response returns immediately without timing out
        new Thread(() -> {
            try {
                System.out.println("[BOOTSTRAP] Starting initial snapshot for tables: " + targetTables.keySet());
                syncService.bootstrapAndStart(targetTables);
                System.out.println("[BOOTSTRAP] Snapshot completed. CDC engine is now streaming.");
            } catch (Exception e) {
                System.err.println("[BOOTSTRAP] Failed during bootstrap: " + e.getMessage());
                e.printStackTrace();
            }
        }, "SyncFlow-Bootstrap-Thread").start();

        return ResponseEntity.ok(Map.of(
                "status", "STARTED",
                "message", "Bootstrap snapshot initiated. Existing source rows will be copied to target, then CDC will stream ongoing changes.",
                "tables", targetTables.keySet()
        ));
    }

    @PostMapping("/stop")
    public Map<String, String> stopSync() {
        syncService.stop();
        return Map.of("message", "CDC engine stop requested");
    }

    @GetMapping("/progress")
    public Map<String, Object> getProgress() {
        ProgressTracker tracker = syncService.getProgressTracker();

        return Map.of(
                "totalEvents", tracker.getTotalEvents(),
                "successfulEvents", tracker.getSuccessfulEvents(),
                "failedEvents", tracker.getFailedEvents(),
                "successRatePercent", tracker.getSuccessRate()
        );
    }

    @GetMapping("/lag")
    public Map<String, Object> getLag() {
        LagMonitor monitor = syncService.getLagMonitor();

        return Map.of(
                "lagMillis", monitor.getCurrentLagMillis(),
                "lagSeconds", monitor.getCurrentLagMillis() / 1000.0,
                "alertRequired", monitor.isAlertRequired(),
                "alertThresholdMillis", monitor.getAlertThresholdMillis()
        );
    }

    @GetMapping("/events")
    public Map<String, Object> getRecentEvents() {
        List<ChangeEvent> events =
                syncService.getEventBuffer().getRecentEvents();

        return Map.of(
                "count", events.size(),
                "events", events
        );
    }
}