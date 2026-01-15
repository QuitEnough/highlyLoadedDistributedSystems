# Загружаем переменные из .env файла
include .env
export

DOCKER_COMPOSE = cd infrastructure && docker compose --env-file ../.env -f docker-compose.yaml

# Используем переменные из .env
KEYCLOAK_URL = http://localhost:${KEYCLOAK_PORT}
KEYCLOAK_INTERNAL_URL = http://keycloak:8080

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
	powershell -Command "$$count=0; while ($$count -lt 60) { try { $$response = Invoke-WebRequest -UseBasicParsing -Uri '$(KEYCLOAK_URL)/health/ready' -ErrorAction Stop; if ($$response.StatusCode -eq 200) { Write-Host 'Keycloak is ready!'; break } } catch { Write-Host 'Keycloak not ready, sleeping...'; Start-Sleep -Seconds 5; $$count++ } }"
else
	@count=0; while [ $$count -lt 60 ]; do \
		if curl -sf '$(KEYCLOAK_URL)/health/ready' > /dev/null 2>&1; then \
			echo "Keycloak is ready!"; \
			break; \
		else \
			echo "Keycloak not ready, sleeping..."; \
			sleep 5; \
			count=$$((count + 1)); \
		fi; \
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
	@echo "Stopping infrastructure services..."
	$(DOCKER_COMPOSE) stop $(INFRA_SERVICES)

clean: stop
	@echo "Cleaning up Docker resources..."
	$(DOCKER_COMPOSE) rm -f
	@echo "Pruning Docker volumes..."
	docker volume prune -f

reset: clean up

rebuild: clean all

# Дополнительные команды для удобства
restart:
	@echo "Restarting all services..."
	$(DOCKER_COMPOSE) restart

infra-restart:
	@echo "Restarting infrastructure services..."
	$(DOCKER_COMPOSE) restart $(INFRA_SERVICES)

down:
	@echo "Stopping and removing all containers, networks, volumes..."
	$(DOCKER_COMPOSE) down -v

pull:
	@echo "Pulling latest images for all services..."
	$(DOCKER_COMPOSE) pull

build:
	@echo "Building images (if any)..."
	$(DOCKER_COMPOSE) build

# Мониторинг
status:
	$(DOCKER_COMPOSE) ps --all

# Для разработки
dev-up:
	@echo "Starting in development mode..."
	$(DOCKER_COMPOSE) up

dev-down:
	@echo "Stopping development mode..."
	$(DOCKER_COMPOSE) down

# Проверка подключения к сервисам
check-postgres:
	@echo "Checking PostgreSQL connection..."
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -h localhost -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "\l" || echo "PostgreSQL is not available"

check-redis:
	@echo "Checking Redis connection..."
	@redis-cli -h localhost -p $(REDIS_PORT) -a $(REDIS_PASSWORD) ping || echo "Redis is not available"

check-minio:
	@echo "Checking MinIO connection..."
	@curl -f http://localhost:$(MINIO_API_PORT)/minio/health/live || echo "MinIO is not available"

check-keycloak:
	@echo "Checking Keycloak health..."
	@curl -f $(KEYCLOAK_URL)/health/ready || echo "Keycloak is not ready"

check-kafka:
	@echo "Checking Kafka connection..."
	@kafka-topics --bootstrap-server localhost:$(KAFKA_PORT) --list || echo "Kafka is not available"

check-prometheus:
	@echo "Checking Prometheus health..."
	@curl -f http://localhost:$(PROMETHEUS_PORT)/-/healthy || echo "Prometheus is not available"

check-grafana:
	@echo "Checking Grafana health..."
	@curl -f http://localhost:$(GRAFANA_PORT)/api/health || echo "Grafana is not available"

check-all: check-postgres check-redis check-minio check-keycloak check-kafka check-prometheus check-grafana
	@echo "All health checks completed!"

# Отображение URL сервисов
urls:
	@echo "Service URLs:"
	@echo "Keycloak:          $(KEYCLOAK_URL)"
	@echo "PostgreSQL:        localhost:$(POSTGRES_PORT)"
	@echo "PostgreSQL (KC):   localhost:$(POSTGRES_PORT_KEYCLOAK)"
	@echo "ClickHouse:        localhost:$(CLICKHOUSE_PORT)"
	@echo "Kafka:             localhost:$(KAFKA_PORT)"
	@echo "Schema Registry:   localhost:$(SCHEMA_REGISTRY_PORT)"
	@echo "Redis:             localhost:$(REDIS_PORT)"
	@echo "MinIO Console:     localhost:$(MINIO_CONSOLE_PORT)"
	@echo "MinIO API:         localhost:$(MINIO_API_PORT)"
	@echo "Camunda:           localhost:$(CAMUNDA_PORT)"
	@echo "Prometheus:        localhost:$(PROMETHEUS_PORT)"
	@echo "Grafana:           localhost:$(GRAFANA_PORT)"
	@echo "Loki:              localhost:$(LOKI_PORT)"
	@echo "Tempo:             localhost:$(TEMPO_PORT)"
	@echo "Alloy:             localhost:$(ALLOY_HTTP_PORT)"

# Просмотр логов отдельных сервисов
logs-keycloak:
	$(DOCKER_COMPOSE) logs -f keycloak

logs-kafka:
	$(DOCKER_COMPOSE) logs -f kafka

logs-postgres:
	$(DOCKER_COMPOSE) logs -f postgres

logs-prometheus:
	$(DOCKER_COMPOSE) logs -f prometheus

logs-grafana:
	$(DOCKER_COMPOSE) logs -f grafana

logs-clickhouse:
	$(DOCKER_COMPOSE) logs -f clickhouse

logs-camunda:
	$(DOCKER_COMPOSE) logs -f camunda

# Переменные окружения
env-list:
	@echo "Current environment variables:"
	@echo "POSTGRES_PORT: $(POSTGRES_PORT)"
	@echo "KEYCLOAK_PORT: $(KEYCLOAK_PORT)"
	@echo "KAFKA_PORT: $(KAFKA_PORT)"
	@echo "GRAFANA_PORT: $(GRAFANA_PORT)"
	@echo "MINIO_API_PORT: $(MINIO_API_PORT)"
	@echo "CAMUNDA_PORT: $(CAMUNDA_PORT)"

# Инициализация (копирование .env примера если нет основного)
init:
	@if [ ! -f .env ]; then \
		echo "Copying .env.example to .env..."; \
		cp .env.example .env; \
		echo "Please edit .env file with your configuration"; \
	else \
		echo ".env file already exists"; \
	fi