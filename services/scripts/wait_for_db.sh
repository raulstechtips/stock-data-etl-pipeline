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
