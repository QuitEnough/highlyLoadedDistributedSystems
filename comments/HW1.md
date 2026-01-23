# Домашняя работа №1: Настройка окружения проекта

## 📋 Задание
**Цель:** Подготовить архитектурное описание и инженерную среду, на которой в дальнейшем будет строиться распределённая микросервисная система.

### Что нужно было сделать:

1. **Создать структуру репозитория:**
```
iot-platform-<group>/name
├── diagrams/ # C4-диаграммы
│ ├── context.puml
│ └── containers.puml
├── infrastructure/ # Инфраструктура Docker
│ └── docker-compose.yaml
├── README.md
├── Makefile
└── .env # Переменные окружения
```


2. **Нарисовать архитектуру в формате C4 (PlantUML):**
    - `context.puml` — системный контекст
    - `containers.puml` — уровень контейнеров
    - Использовать `!includeurl` из C4-PlantUML
    - Показать Kafka, Redis, PostgreSQL, Clickhouse, MinIO, Keycloak, Camunda как инфраструктурные блоки

3. **Подготовить `docker-compose.yaml`:**
    - Все необходимые сервисы (Postgres, Redis, ClickHouse, Kafka, Schema Registry, MinIO, Keycloak, Camunda, Grafana+Prometheus+Tempo+Loki+Alloy)
    - Добавить healthcheck-и, порты, volume'ы, `depends_on`
    - Создать `.env` для настройки портов, логинов, паролей

4. **Создать `Makefile`** для быстрого запуска инфраструктуры

5. **Обновить `README.md`** с инструкциями по запуску

---

## 🗣️ **Мои комментарии:**

### Первая версия:
**Добрый день!**

Выполнила первое дз:
- нарисована архитектура в формате C4 (PlantUML) двух первых уровней,
- подготовлен "docker-compose.yaml",
- создан ".env" для настройки портов, логинов, паролей,
- присутствует Makefile, который позволяет быстро запустить всю инфраструктуру,
- обновлен "README.md" с описанием проекта и "быстрым стартом",
- добавлены дашборды Grafana для Kafka и PostgreSQL.

В ходе выполнения до первого коммита не было функции создать новую ветку. В процессе забыла и закоммитила в main. Уже после создала ветку для merge request-а. Прошу понять и простить)

**Ссылка на MR:** https://gitlab.proselyte.net/ourcode-iot-quebec/slf4u0/-/merge_requests/1

---

## 👨‍🏫 **Ревью от преподавателя:**

**Привет, Яна!**

Самое главное - по диаграммам - мы отображаем только ФАКТИЧЕСКОЕ СОСТОЯНИЕ - с loki, camunda, clickhouse и т.д.
Сейчас у тебя там только копия того, что было в материалах для изучения.

### ✅ **Что хорошо:**

1. **Репозиторий содержит все ключевые артефакты** для Модуля 1: диаграммы C4, docker-compose, Makefile, README и .env.example.

2. **Context-диаграмма** подключает C4-PlantUML через `!includeurl` и показывает акторов/внешние системы (Keycloak, Alloy, Grafana) и основные связи.

3. **Docker Compose** поднимает требуемый минимум инфраструктуры: Postgres, Kafka, Schema Registry, Redis, MinIO, Keycloak, Camunda, ClickHouse, Prometheus, Grafana, Loki, Tempo, Alloy + exporters.

4. **Kafka** настроен с отдельным internal listener (29092) и external listener (9092) + advertised listeners, что снижает боль подключения клиентов.

5. **Есть provisioning Grafana** (datasources + dashboards provider) и два дашборда (Kafka и Postgres) как артефакты сдачи.

6. **Prometheus** уже настроен на scrape Kafka Exporter и Postgres Exporter, а также на Keycloak/Grafana/Loki/Tempo/Alloy.

### ❌ **Что нужно исправить:**

#### 🔴 **BLOCKER:**

**BLOCKER-1**
- **Где:** `infrastructure/docker-compose.yaml:95`
- **Проблема:** bind-mount для импорта realm в Keycloak указывает на `./infrastructure/keycloak/realm-config.json`, но compose-файл лежит в `infrastructure/`, значит путь резолвится как `infrastructure/infrastructure/keycloak/...` и файла там нет.
- **Риск:** Keycloak поднимется без требуемого realm/клиентов/ролей.

**BLOCKER-2**
- **Где:** `diagrams/containers.puml:2, 15–17, 19–25`
- **Проблема:** контейнерная диаграмма не соответствует требованиям задания:
    - Подключение C4-PlantUML через `!include` вместо `!includeurl`
    - Event Service "читает события из Cassandra", хотя используется ClickHouse
    - Отсутствуют обязательные инфраструктурные блоки/связи для Avro-контрактов и observability
    - Заложены два PostgreSQL (Devices/Commands), которых нет в docker-compose
- **Риск:** контейнерная C4-диаграмма не засчитывается.

#### 🟡 **HIGH / MEDIUM / LOW:**

**HIGH-1**
- **Где:** `README.md:94–98`
- **Проблема:** команда "Ручной запуск" написана неверно — отсутствует `up` и неверный порядок флагов.
- **Риск:** не смогу запустить окружение по README.

**MEDIUM-1**
- **Где:** `infrastructure/prometheus/prometheus.yml:45–48`
- **Проблема:** scrape job `person-postgres` указывает на `person-postgres-exporter:9187`, но такого сервиса нет.
- **Риск:** Prometheus Targets будут "красными".

**MEDIUM-2**
- **Где:** `infrastructure/docker-compose.yaml:43–47`
- **Проблема:** healthcheck Redis задан некорректно для `CMD-SHELL`.
- **Риск:** Redis может показываться как unhealthy.

**MEDIUM-3**
- **Где:** `infrastructure/docker-compose.yaml:24, 106, 125, 226, 271`
- **Проблема:** используется `:latest` для ключевых компонентов.
- **Риск:** ломает воспроизводимость.

**LOW-1**
- **Где:** `infrastructure/docker-compose.yaml:332–334`
- **Проблема:** volume `schema_registry_data` объявлен, но не используется.
- **Риск:** мусор в конфиге.

### 📝 **Итог:**
**На доработку.** Есть блокирующие проблемы: некорректный mount realm-конфига Keycloak и контейнерная C4-диаграмма не соответствует требованиям задания.

---

## 🗣️ **Мои комментарии после исправлений:**

**Евгений, добрый вечер!**

Спасибо большое за подробное описание правок. Вот исправленная версия: https://gitlab.proselyte.net/ourcode-iot-quebec/slf4u0/-/merge_requests/1

---

## 👨‍🏫 **Финальное ревью от преподавателя:**

**Привет, Яна!**

Все отлично, закрываем модуль. ✅

---

## 📊 **Статус задания:**
✅ **ВЫПОЛНЕНО И ПРИНЯТО**