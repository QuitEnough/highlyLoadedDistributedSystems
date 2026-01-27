package com.slf4u0.emulatorservice.controller;

import com.slf4u0.emulatorservice.service.MessageGeneratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/emulator")
@RequiredArgsConstructor
public class EmulatorController {

    private final MessageGeneratorService messageGeneratorService;

    @PostMapping("/send-messages")
    public ResponseEntity<String> sendMessages(@RequestParam(defaultValue = "100") int count) {
        new Thread(() -> messageGeneratorService.sendMultipleMessages(count)).start();
        return ResponseEntity.ok("Started sending " + count + " messages in the background.");
    }

    @PostMapping("/send-million-messages")
    public ResponseEntity<String> sendMillionMessages() {
        new Thread(() -> messageGeneratorService.sendMultipleMessages(1000000)).start();
        return ResponseEntity.ok("Started sending 1,000,000 messages in the background.");
    }

    @PostMapping("/send-controller-data")
    public ResponseEntity<String> sendControllerData(
            @RequestParam(defaultValue = "default-controller") String controllerId,
            @RequestParam(defaultValue = "TELEMETRY_DATA") String eventType,
            @RequestParam(defaultValue = "{\"temperature\": 25.0}") String payload) {

        messageGeneratorService.sendControllerData(controllerId, eventType, payload);
        return ResponseEntity.ok("Sent controller data to Kafka: " + controllerId);
    }

    @PostMapping("/send-script-data")
    public ResponseEntity<String> sendScriptData(
            @RequestParam(defaultValue = "default-device") String deviceId,
            @RequestParam(defaultValue = "DEVICE_EVENT") String eventType,
            @RequestParam(defaultValue = "{\"status\": \"active\"}") String payload) {

        messageGeneratorService.sendScriptData(deviceId, eventType, payload);
        return ResponseEntity.ok("Sent script data to Kafka: " + deviceId);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Emulator service is running.");
    }

}
