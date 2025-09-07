# Flink Deduplication Project

Apache Flink проект для дедупликации данных с поддержкой TTL (Time To Live).

## Структура проекта

```
flink-dedup/
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── visa/
│       │           └── flink/
│       │               ├── dedup/                    # Логика дедупликации
│       │               │   ├── DedupWithTTL.java
│       │               │   └── ConfluentAvroToMapDeserializationSchema.java
│       │               ├── model/                    # Модели данных
│       │               │   ├── ConsumerGenericRecord.java
│       │               │   └── Payload.java
│       │               ├── serialization/            # Сериализация
│       │               │   ├── EventTimeKafkaSerializer.java
│       │               │   └── JsonMapValueSerializer.java
│       │               ├── sink/                     # Sink'и для вывода
│       │               │   ├── ClickHouseDynamicSink3.java
│       │               │   ├── ClickHouseDynamicSink4.java
│       │               │   └── KafkaSinkFactory.java
│       │               └── utils/                    # Утилиты
│       │                   ├── AppLogger.java
│       │                   ├── DedupUtils.java
│       │                   ├── JobConfigLoader.java
│       │                   ├── FlinkConsumerUnionDedupTTLJob.java
│       │                   ├── FlinkJobRunner.java
│       │                   └── SchemaRegistrySubjectFetcher.java
│       └── resources/
│           ├── schema/                               # Схемы данных
│           ├── vvp/                                 # VVP конфигурации
│           ├── all.properties                       # Общие настройки
│           ├── app.properties                       # Настройки приложения
│           ├── click_house_table_hades_dedup        # Схема таблицы ClickHouse
│           ├── dev.properties                       # Настройки DEV среды
│           ├── qa.properties                        # Настройки QA среды
│           └── job.json                             # Конфигурация задания
├── target/                                          # Собранные артефакты
└── pom.xml                                          # Maven конфигурация
```

## Основные компоненты

### DedupWithTTL

Основной класс для дедупликации с поддержкой TTL.

### JsonMapValueSerializer

Кастомный сериализатор для преобразования ConsumerGenericRecord в JSON.

### KafkaSinkFactory

Фабрика для создания Kafka sink'ов с различными настройками.

### FlinkConsumerUnionDedupTTLJob

Главный класс Flink задания, который:

- Читает данные из нескольких Kafka топиков
- Объединяет потоки данных
- Применяет дедупликацию с TTL
- Записывает результат в выходной Kafka топик

## Сборка и запуск

### Сборка проекта

```bash
mvn clean package
```

### Запуск задания

```bash
java -jar target/flink-dedup-1.0.0.jar [path/to/job.json]
```

## Конфигурация

Основные настройки находятся в файле `job.json`:

```json
{
  "dedup": {
    "ttl.ms": 300000
  },
  "qa": {
    "bootstrap.servers": "localhost:9092",
    "topic": "qa-topic"
  },
  "dev": {
    "bootstrap.servers": "localhost:9092",
    "topic": "dev-topic"
  },
  "kafka-sink": {
    "bootstrap.servers": "localhost:9092",
    "topic": "output-topic"
  }
}
```

## Зависимости

- Apache Flink 1.17.1
- Apache Kafka 3.4.0
- Jackson 2.15.2
- Confluent Schema Registry 7.4.0
- ClickHouse JDBC 0.4.6

## Статус реализации

- [x] Реализована логика в DedupWithTTL
- [x] Добавлена реализация ConfluentAvroToMapDeserializationSchema
- [x] Завершены модели ConsumerGenericRecord и Payload
- [x] Реализован JsonMapValueSerializer
- [x] Реализованы утилиты (AppLogger, JobConfigLoader)
- [x] Настроены конфигурационные файлы
- [x] Создан рабочий FlinkConsumerUnionDedupTTLJob
- [ ] Реализовать EventTimeKafkaSerializer
- [ ] Добавить ClickHouse sink'и
- [ ] Реализовать DedupUtils и SchemaRegistrySubjectFetcher

## Особенности реализации

### Дедупликация с TTL

Проект использует Flink state для отслеживания дубликатов с настраиваемым TTL. При первом появлении ключа запись эмитируется, последующие записи с тем же ключом игнорируются до истечения TTL.

### Поддержка Confluent Schema Registry

Проект поддерживает чтение Avro сообщений из Kafka с использованием Confluent Schema Registry для автоматического получения схем.

### Конфигурация через JSON

Все настройки Kafka, TTL и других параметров задаются через файл `job.json`, что обеспечивает гибкость развертывания.
