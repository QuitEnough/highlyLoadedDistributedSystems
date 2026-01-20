Привет, Яна!
Самое главное - по диаграммам - мы отображаем только ФАКТИЧЕСКОЕ СОСТОЯНИЕ - с loki, camunda, clickhouse и т.д.
Сейчас у тебя там только копия того, что было в материалах для изучения.

Что хорошо

Репозиторий содержит все ключевые артефакты для Модуля 1: диаграммы C4, docker-compose, Makefile, README и .env.example. (README.md:42–60; diagrams/context.puml:1–24; diagrams/containers.puml:1–53; infrastructure/docker-compose.yaml:1–334; Makefile:1–109; .env.example:1–59)

Context-диаграмма подключает C4-PlantUML через !includeurl и показывает акторов/внешние системы (Keycloak, Alloy, Grafana) и основные связи. (diagrams/context.puml:1–23)

Docker Compose поднимает требуемый минимум инфраструктуры: Postgres, Kafka, Schema Registry, Redis, MinIO, Keycloak, Camunda, ClickHouse, Prometheus, Grafana, Loki, Tempo, Alloy + exporters. (infrastructure/docker-compose.yaml:5–324)

Kafka настроен с отдельным internal listener (29092) и external listener (9092) + advertised listeners, что снижает боль подключения клиентов. (infrastructure/docker-compose.yaml:164–191)

Есть provisioning Grafana (datasources + dashboards provider) и два дашборда (Kafka и Postgres) как артефакты сдачи. (infrastructure/grafana/provisioning/datasources/datasources.yaml:1–20; infrastructure/grafana/provisioning/dashboards/dashboards.yml:1–10; infrastructure/grafana/dashboards/kafka-overview.json:1–80; infrastructure/grafana/dashboards/postgres-overview.json:1–42)

Prometheus уже настроен на scrape Kafka Exporter и Postgres Exporter, а также на Keycloak/Grafana/Loki/Tempo/Alloy. (infrastructure/prometheus/prometheus.yml:1–44)

Что нужно исправить

BLOCKER

BLOCKER-1

Где: infrastructure/docker-compose.yaml:95

Почему это проблема: bind-mount для импорта realm в Keycloak указывает на ./infrastructure/keycloak/realm-config.json, но compose-файл лежит в infrastructure/, значит путь резолвится как infrastructure/infrastructure/keycloak/... и файла там нет. В лучшем случае импорт realm не произойдет, в худшем Docker создаст директорию вместо файла и Keycloak получит некорректный mount.

Риск, если не исправлять: Keycloak поднимется без требуемого realm/клиентов/ролей (или не поднимется корректно), а проверка security части окружения станет невозможной.

BLOCKER-2

Где: diagrams/containers.puml:2, 15–17, 19–25

Почему это проблема: контейнерная диаграмма не соответствует требованиям задания и частично не соответствует реальной инфраструктуре.

Подключение C4-PlantUML сделано через !include с URL (diagrams/containers.puml:2), что часто не рендерится в PlantUML без !includeurl.

В диаграмме Event Service “читает события из Cassandra” (diagrams/containers.puml:15), хотя по заданию и docker-compose используется ClickHouse (infrastructure/docker-compose.yaml:144–163).

На диаграмме отсутствуют обязательные инфраструктурные блоки/связи для Avro-контрактов и observability (Schema Registry/Prometheus/Grafana/Loki/Tempo как часть модели контейнеров), а также заложены два PostgreSQL (Devices/Commands), которых нет в docker-compose. (diagrams/containers.puml:21–22 vs infrastructure/docker-compose.yaml:5–22, 49–65)

Риск, если не исправлять: контейнерная C4-диаграмма не засчитывается и “архитектурное описание” формально проваливает модуль.

HIGH / MEDIUM / LOW

HIGH-1

Где: README.md:94–98

Почему проблема: команда “Ручной запуск” написана неверно — отсутствует up и неверный порядок флагов docker compose ... -d). Это неработающий Quick Start сценарий для проверяющего/студента.

Риск: не смогу запустить окружение по README, даже если compose корректный.

MEDIUM-1

Где: infrastructure/prometheus/prometheus.yml:45–48

Почему проблема: scrape job person-postgres указывает на person-postgres-exporter:9187, но такого сервиса в docker-compose нет. Это явный “хвост” копипаста и гарантированный DOWN target.

Риск: Prometheus Targets будут “красными” без причины, ухудшая доверие к наблюдаемости и усложняя проверку.

MEDIUM-2

Где: infrastructure/docker-compose.yaml:43–47

Почему проблема: healthcheck Redis задан как ["CMD-SHELL", "redis-cli", "-a", "...", "ping"]. Для CMD-SHELL ожидается одна строка-команда (обычно второй элемент массива), а не разнесенные аргументы; в зависимости от реализации Docker это может работать некорректно.

Риск: Redis может показываться как unhealthy без реальной проблемы, а требование “healthcheck-и” формально не выполнено.

MEDIUM-3

Где: infrastructure/docker-compose.yaml:24, 106, 125, 226, 271

Почему проблема: используется :latest для ключевых компонентов (postgres-exporter, minio, camunda, prometheus, alloy). Это ломает воспроизводимость: одинаковое ДЗ сегодня и через неделю может “внезапно” перестать подниматься.

Риск: нестабильность окружения и невалидируемость результата (особенно на проверке).

LOW-1

Где: infrastructure/docker-compose.yaml:332–334

Почему проблема: volume schema_registry_data объявлен, но нигде не используется сервисом schema-registry.

Риск: мусор в конфиге, снижает качество и усложняет сопровождение.

Итог

На доработку. Есть блокирующие проблемы: некорректный mount realm-конфига Keycloak (Keycloak не гарантированно поднимется в нужной конфигурации). Дополнительно контейнерная C4-диаграмма не соответствует требованиям задания (include не по стандарту, Cassandra вместо ClickHouse, отсутствуют обязательные инфраструктурные блоки/связи и расхождение с compose).