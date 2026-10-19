
package com.syncflow.controller;

import com.syncflow.monitoring.LagMonitor;
import com.syncflow.monitoring.ProgressTracker;
import com.syncflow.service.SyncService;
import org.springframework.web.bind.annotation.*;
import com.syncflow.event.ChangeEvent;
import java.util.List;

import java.util.Map;

@RestController
@RequestMapping("/api/sync")
public class SyncController {

    private final SyncService syncService;

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

    @PostMapping("/start")
    public Map<String, String> startSync() {
        if (syncService.isRunning()) {
            return Map.of("message", "SyncFlow is already running");
        }

        syncService.start();
        return Map.of("message", "CDC engine start requested");
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
