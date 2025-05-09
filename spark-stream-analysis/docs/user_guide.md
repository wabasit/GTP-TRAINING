# User Guide

## Overview
This guide explains how to set up, run, and monitor the e-commerce event streaming pipeline using Spark Streaming and PostgreSQL.

## Prerequisites
- **Docker and Docker Compose**: Installed on your system.
- **PostgreSQL JDBC Driver**: Download `postgresql-42.7.3.jar` and place it in the `jars/` directory.
- **pgAdmin (Optional)**: For database visualization.
- **Directory Structure**:
  - `notebooks/`: Contains `data_generator.py`, `spark_streaming_to_postgres.py`, `postgres_connection.json`.
  - `data/`: For generated CSV files.
  - `jars/`: For the JDBC driver.
  - `Dockerfile.pyspark` and `docker-compose.yml`.

## Setup

### 1. Configure PostgreSQL
- **Option 1: Use Dockerized PostgreSQL**:
  - Ensure `docker-compose.yml` includes the `postgres` service (port `5433:5432` to avoid conflicts).
  - Connect pgAdmin to `localhost:5433`, database `ecommerce_db`, user `postgres`, password `postgres`.
- **Option 2: Use Local PostgreSQL**:
  - Create a database `ecommerce_db` on your local PostgreSQL server.
  - Update `postgres_connection.json`:
    ```json
    {
      "jdbc": {
        "url": "jdbc:postgresql://host.docker.internal:5432/ecommerce_db",
        "user": "postgres",
        "password": "your_local_password",
        "driver": "org.postgresql.Driver",
        "properties": {
          "batchSize": 500,
          "maximumPoolSize": 10
        }
      }
    }