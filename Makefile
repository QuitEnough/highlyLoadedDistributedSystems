include .env
export

DOCKER_COMPOSE = cd infrastructure && docker compose --env-file ../.env -f docker-compose.yaml

KEYCLOAK_URL = http://localhost:${KEYCLOAK_PORT}
KEYCLOAK_INTERNAL_URL = http://keycloak:8080

INFRA_SERVICES ?= \
	postgres \
	kafka \
	schema-registry \
	redis \
	minio \
	keycloak \
	camunda \
	clickhouse \
	prometheus \
	grafana \
	loki \
	tempo \
	alloy \
	postgres-exporter \
	keycloak-postgres-exporter \
	kafka-exporter \
    redis-exporter \
    schema-init

.PHONY: all up start stop clean logs ps reset infra infra-logs infra-stop rebuild emulator emulator-build send-million-messages

all: up

ifeq ($(OS),Windows_NT)
WAIT_KEYCLOAK_CMD = powershell -Command "while ($$true) { \
		try { \
			Invoke-WebRequest -UseBasicParsing -Uri '$(KEYCLOAK_URL)/health/ready' -ErrorAction Stop; \
			break \
		} \
		catch { \
			Write-Host 'Keycloak not ready, sleeping...'; \
			Start-Sleep -Seconds 5 \
		} \
	}"
else
WAIT_KEYCLOAK_CMD = until curl -sf '$(KEYCLOAK_URL)/health/ready'; do \
	echo 'Keycloak not ready, sleeping...'; sleep 5; \
	done
endif

ifeq ($(OS),Windows_NT)
WAIT_KAFKA_CMD = powershell -Command "$$count=0; while ($$count -lt 30) { \
		try { \
			$$output = docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>&1; \
			if ($$LASTEXITCODE -eq 0) { break } \
		} catch { } \
		Write-Host 'Kafka not ready, sleeping...'; \
		Start-Sleep -Seconds 5; \
		$$count++ \
	}"
else
WAIT_KAFKA_CMD = @count=0; while [ $$count -lt 30 ]; do \
		if docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then \
			break; \
		fi; \
		echo 'Kafka not ready, sleeping...'; \
		sleep 5; \
		count=$$((count + 1)); \
	done
endif

up:
	@echo "Starting full IoT platform..."
	$(DOCKER_COMPOSE) up -d

infra:
	@echo "Starting infrastructure services: $(INFRA_SERVICES)"
	$(DOCKER_COMPOSE) up -d $(INFRA_SERVICES)
	@echo "Waiting for Keycloak to be ready..."
	@$(WAIT_KEYCLOAK_CMD) || echo "Warning: Keycloak healthcheck failed, continuing anyway..."
	@echo "Waiting for Kafka to be ready..."
	@$(WAIT_KAFKA_CMD)
	@echo "Infrastructure is ready!"

ps:
	$(DOCKER_COMPOSE) ps

logs:
	$(DOCKER_COMPOSE) logs -f --tail=100

infra-logs:
	$(DOCKER_COMPOSE) logs -f --tail=100 $(INFRA_SERVICES)

stop:
	@echo "Stopping all services..."
	$(DOCKER_COMPOSE) down

infra-stop:
	@echo "Stopping infrastructure services..."
	$(DOCKER_COMPOSE) stop $(INFRA_SERVICES)

clean: stop
	@echo "Cleaning up Docker resources..."
	$(DOCKER_COMPOSE) rm -f
	@echo "Pruning Docker volumes..."
	docker volume prune -f

reset: clean up

rebuild: clean all

down:
	@echo "Stopping and removing all containers, networks, volumes..."
	$(DOCKER_COMPOSE) down -v

ifeq ($(OS),Windows_NT)
GRADLEW = gradlew.bat
else
GRADLEW = ./gradlew
endif

emulator-build:
	@echo "Building emulator service..."
	cd services/emulator-service && $(GRADLEW) build -x test

emulator:
	@echo "Starting emulator service..."
	cd services/emulator-service && $(GRADLEW) bootRun

send-million-messages:
	@echo "Sending 1,000,000 messages to Kafka..."
	bash infrastructure/send_million_messages.sh