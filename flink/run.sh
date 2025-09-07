#!/bin/bash

# Скрипт для запуска Flink дедупликации
# Использование: ./run.sh [path/to/job.json]

set -e

# Определяем путь к конфигурации
JOB_CONFIG=${1:-"src/main/resources/job.json"}

# Проверяем существование файла конфигурации
if [ ! -f "$JOB_CONFIG" ]; then
    echo "Ошибка: Файл конфигурации $JOB_CONFIG не найден"
    exit 1
fi

# Создаем директорию для логов
mkdir -p logs

# Собираем проект
echo "Сборка проекта..."
mvn clean package -DskipTests

# Проверяем успешность сборки
if [ ! -f "target/flink-dedup-1.0.0.jar" ]; then
    echo "Ошибка: Не удалось собрать проект"
    exit 1
fi

echo "Запуск Flink задания с конфигурацией: $JOB_CONFIG"
echo "Логи будут записаны в директорию: logs/"

# Запускаем Flink задание
java -jar target/flink-dedup-1.0.0.jar "$JOB_CONFIG"
