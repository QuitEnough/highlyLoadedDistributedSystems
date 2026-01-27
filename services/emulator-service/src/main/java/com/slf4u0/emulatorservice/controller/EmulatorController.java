package com.slf4u0.emulatorservice.controller;

import com.slf4u0.emulatorservice.service.EmulatorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/emulator")
@RequiredArgsConstructor
public class EmulatorController {

    private EmulatorService emulatorService;

    @PostMapping("/controller/{controllerId}")
    public ResponseEntity<String> sendControllerData(
            @PathVariable String controllerId,
            @RequestParam(defaultValue = "TELEMETRY") String eventType,
            @RequestBody String payload
    ) {
        emulatorService.sendControllerData(controllerId, eventType, payload);
        return ResponseEntity.ok("Controller data sent to Kafka for device: " + controllerId);
    }

    @PostMapping("/script/{deviceId}")
    public ResponseEntity<String> sendScriptData(
            @PathVariable String deviceId,
            @RequestParam(defaultValue = "SCRIPT_EXECUTION") String eventType,
            @RequestBody String payload) {

        emulatorService.sendScriptData(deviceId, eventType, payload);
        return ResponseEntity.ok("Script data sent to Kafka for device: " + deviceId);
    }

    @GetMapping("/test-data")
    public ResponseEntity<String> triggerTestData() {
        // Trigger the periodic method manually for testing
        emulatorService.sendPeriodicDataToKafka();
        return ResponseEntity.ok("Test data sent to Kafka");
    }

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        return ResponseEntity.ok("Emulator service is running");
    }

}
