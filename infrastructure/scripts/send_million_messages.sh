#!/bin/bash

# Script to send a million messages to Kafka via the emulator service
# This script assumes that both the infrastructure (Kafka) and the emulator service are running

set -e  # Exit immediately if a command exits with a non-zero status

echo "Sending 1,000,000 messages to Kafka..."

# Check if emulator service is running
if ! curl -s http://localhost:8085/api/emulator/health >/dev/null 2>&1; then
    echo "Error: Emulator service is not running on port 8085"
    echo "Please start the emulator service first:"
    echo "  cd /workspace/services/emulator-service && ./gradlew bootRun"
    exit 1
fi

echo "Emulator service is running. Sending 1,000,000 messages..."

# Send the request to trigger the million messages
response=$(curl -s -X POST "http://localhost:8085/api/emulator/send-million-messages")

echo "$response"

echo "Message sending initiated. The process runs in the background."
echo "Check the emulator service logs for progress updates."

# Optional: Wait for a few seconds to ensure the request was processed
sleep 3

echo "Done!"