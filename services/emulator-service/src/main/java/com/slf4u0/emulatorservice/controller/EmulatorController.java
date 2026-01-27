package com.slf4u0.emulatorservice.controller;

import com.slf4u0.emulatorservice.service.EmulatorService;
import com.slf4u0.emulatorservice.service.MessageGeneratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/emulator")
@RequiredArgsConstructor
public class EmulatorController {

    private MessageGeneratorService messageGeneratorService;

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

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Emulator service is running.");
    }

}
