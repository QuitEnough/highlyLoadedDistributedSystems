DOCKER_COMPOSE = docker compose -f infrastructure/docker-compose.yaml

INFRA_SERVICES ?= \
	postgres \
	cassandra \
	zookeeper \
	kafka \
	schema-registry \
	redis \
	minio \
	keycloak \
	camunda \
	prometheus \
	grafana \
	loki \
	tempo \
	alloy \
	postgres-exporter \
	kafka-exporter

KEYCLOAK_URL = http://localhost:8080

.PHONY: all up start stop clean logs ps reset infra infra-logs infra-stop rebuild

all: up

up:
	@echo "Starting full IoT platform..."
	$(DOCKER_COMPOSE) up -d

infra:
	@echo "Starting infrastructure services: $(INFRA_SERVICES)"
	$(DOCKER_COMPOSE) up -d $(INFRA_SERVICES)
	@echo "Waiting for Keycloak to be ready..."
ifeq ($(OS),Windows_NT)
	powershell -Command "while ($$true) { try { Invoke-WebRequest -UseBasicParsing -Uri $(KEYCLOAK_URL)/health/ready -ErrorAction Stop; break } catch { Write-Host 'Keycloak not ready, sleeping...'; Start-Sleep -Seconds 5 } }"
else
	until curl -sf $(KEYCLOAK_URL)/health/ready; do \
		echo "Keycloak not ready, sleeping..."; sleep 5; \
	done
endif
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
	@echo "🛑 Stopping infrastructure services..."
	$(DOCKER_COMPOSE) stop $(INFRA_SERVICES)

clean: stop
	@echo "Cleaning up Docker resources..."
	$(DOCKER_COMPOSE) rm -f
	docker volume prune -f

reset: clean up

rebuild: clean all