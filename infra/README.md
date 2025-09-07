# Kafka → ClickHouse Connector Setup

## Обзор

Настроен ClickHouse Kafka Sink Connector для потребления JSON данных из Kafka и записи в ClickHouse таблицу `hades.prepared_data_json`.

## Быстрый старт

### 1. Подготовка ClickHouse Connector

```bash
# Скачайте ClickHouse Kafka Sink Connector JAR файлы
# Поместите их в папку:
mkdir -p infra/connect-plugins/clickhouse-kafka-connect/
# Скопируйте JAR файлы в эту папку
```

### 2. Запуск инфраструктуры

```bash
cd infra
docker compose up -d
```

### 3. Тестирование (автоматическое)

```bash
bash scripts/test_end_to_end.sh
```

### 4. Ручное тестирование

```bash
# Создать топик
bash scripts/create_topic.sh

# Отправить тестовые данные
bash scripts/produce_sample_json.sh

# Зарегистрировать коннектор
bash scripts/register_connector.sh

# Проверить данные в ClickHouse
docker exec -it clickhouse clickhouse-client -q "SELECT * FROM hades.prepared_data_json LIMIT 5"
```

## Конфигурации

### Тестовый топик (prepared_data_json)

- Конфиг: `connectors/clickhouse-sink.json`
- Скрипт: `scripts/create_topic.sh`

### Продакшн топик (auth_dsdauthenEnrich)

- Конфиг: `connectors/clickhouse-sink-production.json`
- Скрипт: `scripts/create_production_topic.sh`

## Особенности

- **JSON без схемы**: Неизвестные поля игнорируются (`input_format_skip_unknown_fields=1`)
- **Парсинг времени**: Поддержка различных форматов времени (`date_time_input_format=best_effort`)
- **Обработка ошибок**: Все ошибки отправляются в DLQ топик
- **Масштабирование**: Настраиваемое количество задач коннектора

## Мониторинг

- **Kafka-UI**: http://localhost:8080
- **ClickHouse**: http://localhost:8123
- **Kafka Connect**: http://localhost:8083

## Структура данных

Таблица `hades.prepared_data_json`:

```sql
CREATE TABLE hades.prepared_data_json (
  authId     String,
  dc         String,
  eventTime  DateTime64(3),
  amount     Float64,
  currency   String,
  merchantId String,
  country    String,
  status     String,
  riskScore  Int32,
  cardBin    String,
  deviceType Nullable(String)
) ENGINE = MergeTree
ORDER BY (authId, eventTime);
```

## Следующие шаги

1. Добавить Schema Registry для Avro поддержки
2. Настроить мониторинг и алерты
3. Оптимизировать производительность для продакшн нагрузки
