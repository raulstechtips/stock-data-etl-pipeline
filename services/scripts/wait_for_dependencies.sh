#!/bin/sh

if [ "$DATABASE" = "postgres" ]
then
    echo "Waiting for PostgreSQL..."
    until pg_isready --host=$SQL_HOST --username=$SQL_USER
    do
        echo "Waiting for PostgreSQL..."
        sleep 1
    done
    echo "PostgreSQL started"
fi

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ..."
until nc -z ${RABBITMQ_HOST} ${RABBITMQ_PORT}; do
  echo "RabbitMQ is unavailable - sleeping"
  sleep 2
done
echo "RabbitMQ is up!"

# Wait for Redis to be ready
echo "Waiting for Redis..."
until nc -z ${REDIS_HOST} ${REDIS_PORT}; do
  echo "Redis is unavailable - sleeping"
  sleep 2
done
echo "Redis is up!"