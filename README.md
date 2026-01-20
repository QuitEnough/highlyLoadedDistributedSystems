# 🌐 IoT Monitoring Platform — учебный проект

**Стек**:  
Java 24, Spring Boot 3.5 (WebFlux / Web MVC), Go 1.20+ (Gin/Fiber/net/http), Apache Kafka (Avro + Confluent Schema Registry), PostgreSQL (шардированная через Apache ShardingSphere), ClickHouse, Redis, MinIO, Keycloak (OAuth2/OIDC, JWT), Camunda, Prometheus + Grafana + Loki + Tempo + Alloy, Docker Compose, Testcontainers.

> Репозиторий предназначен для обучения: демонстрирует построение отказоустойчивой, наблюдаемой и безопасной микросервисной платформы для IoT-устройств с асинхронной обработкой событий, шардированием БД, оркестрацией процессов и сквозной трассировкой.

---

## 📚 Содержание

- [Архитектура](#архитектура)
- [Состав репозитория](#состав-репозитория)
- [Быстрый старт](#быстрый-старт)
- [Ручной запуск (без Makefile)](#ручной-запуск-без-makefile)
- [Порты и сервисы](#порты-и-сервисы)
- [Наблюдаемость](#наблюдаемость)
- [Тестирование](#тестирование)

---

## 🏗️ Архитектура

### Диаграммы архитектуры (C4 + PlantUML)

Файлы находятся в каталоге [`diagrams/`](./diagrams/):

- **Context (уровень системы)** — [`diagrams/context.puml`](./diagrams/context.puml)
- **Container (уровень системы)** — [`diagrams/containers.puml`](diagrams/containers.puml)

Рендерить можно любым PlantUML‑совместимым плагином или CI. Файлы используют библиотеку **[C4-PlantUML](https://github.com/plantuml-stdlib/C4-PlantUML)** (включение по URL).

#### Ключевые потоки:
- **IoT-устройства** → отправляют телеметрию в Kafka (AVRO) → **Events Collector** → сохраняет в **ClickHouse**
- **API Orchestrator** → координирует вызовы к **Event Service**, **Device Service**, **Command Service**
- **Command Service (Go)** → отправляет команды устройствам по **gRPC**
- Все сервисы → отправляют метрики и логи в **Grafana Alloy** → **Prometheus/Loki/Tempo** → **Grafana**
- Аутентификация всех запросов через **Keycloak** (OIDC/JWT)

---

## 📁 Состав репозитория

```
iot-platform-/
├── diagrams/ # C4-диаграммы архитектуры
│ ├── context.puml
│ └── containers.puml
├── infrastructure/ # Конфигурации инфраструктуры
│ ├── alloy/
│ ├── grafana/
│ ├── keycloak/
│ ├── loki/
│ ├── prometheus/
│ ├── tempo/
│ └── docker-compose.yaml
├── Makefile # Сценарии автоматизации
├── .env.example # Пример переменных окружения
└── README.md    
```

> Микросервисы (`api-gateway`, `event-service`, `device-service`, `command-service` и др.) могут быть реализованы отдельно, но их сборка и запуск управляются через `Makefile` и `docker-compose`.

---

## 🚀 Быстрый старт

> **Требуется**: Docker (Compose), JDK 24, `make`.

### Настройка окружения

Скопируйте пример файла переменных окружения и при необходимости отредактируйте его:

```bash
cp .env.example .env
# Откройте .env в редакторе и измените пароли/логины по желанию
```
>Все порты, учётные данные и параметры конфигурации определяются в этом файле.

### через `Makefile` (рекомендуется)

```bash
# В корне репозитория
make all
```
**Что делает команда:**
- Поднимает всю инфраструктуру: Postgres, ClickHouse, Kafka, Schema Registry, Redis, MinIO, Keycloak, Camunda, Prometheus, Grafana, Loki, Tempo, Alloy.
- Запускает все микросервисы (если они включены в docker-compose).

### Ручной запуск (без Makefile)

**Запуск инфраструктуры:**

```bash
# В корне репозитория
cd infrastructure
docker compose --env-file ../.env up -d
```

## 🌐 Порты и сервисы
| Сервис | Порт (host) | Описание |
|--------|-------------|----------|
| Keycloak | 8089 | OIDC-провайдер, админка: http://localhost:8089 |
| Grafana | 3000 | Визуализация: http://localhost:3000 (admin/admin) |
| Prometheus | 9090 | Метрики: http://localhost:9090 |
| Loki | 3100 | Логи (без UI, используется через Grafana) |
| Tempo | 3200 | Трассировки (без UI, используется через Grafana) |
| Alloy | 9080 | HTTP endpoint; OTLP: 4317 (gRPC), 4318 (HTTP) |
| MinIO Console | 9001 | Объектное хранилище: http://localhost:9001 |
| Camunda | 8088 | BPMN Engine: http://localhost:8088/camunda |
| ClickHouse | 8123 | HTTP-интерфейс для аналитики |
| PostgreSQL | 5432 | Основная БД (Devices, Commands и др.) |
| Kafka | 9092 | Брокер сообщений (PLAINTEXT) |
| Schema Registry | 8081 | Управление AVRO-схемами |

## 🔍 Наблюдаемость

- **Метрики:** каждый Spring Boot-сервис экспонирует `/actuator/prometheus`.
- **Логи:** собираются через Grafana Alloy → Loki.
- **Трассировки:** OpenTelemetry OTLP → Tempo → Grafana Explore.
- **Grafana** автопровижинит датасорсы из `infrastructure/grafana/provisioning/`.
- **Дашборды:** преднастроенные дашборды для Kafka, PostgreSQL, сервисов и инфраструктуры.

## 🧪 Тестирование

- **Unit-тесты:** JUnit 5 + Mockito.
- **Интеграционные тесты:** Testcontainers (PostgreSQL, Kafka, Keycloak).
- **Архитектурные тесты:** ArchUnit (проверка Hexagonal Architecture).


## ✍️ Автор
[Telegram](https://t.me/slf4u0)
