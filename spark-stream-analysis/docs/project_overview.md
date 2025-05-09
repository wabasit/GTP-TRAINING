# Project Overview

## Project Title
E-commerce Event Streaming Pipeline with Spark and PostgreSQL

## Objective
This project builds a real-time data streaming pipeline to process e-commerce user events (e.g., views, purchases) using Apache Spark Streaming. The pipeline generates synthetic user event data, streams it through Spark, and stores the processed data in a PostgreSQL database for analysis. The goal is to simulate a scalable, fault-tolerant streaming system for e-commerce analytics.

## Architecture
The pipeline consists of the following components:
- **Data Generator**: A Python script (`data_generator.py`) that generates synthetic user event data and writes it to CSV files with timestamped filenames.
- **Spark Streaming Job**: A PySpark script (`spark_streaming_to_postgres.py`) that reads the CSV files in streaming mode, processes the data in micro-batches, and writes the results to a PostgreSQL database.
- **PostgreSQL Database**: Stores the streamed user events in a `user_events` table for downstream analysis.
- **Dockerized Environment**: Uses Docker Compose to orchestrate the services, including Spark (master and worker nodes), a Jupyter notebook environment for PySpark, and (optionally) a PostgreSQL container.

## Key Features
- **Data Generation**: Generates 30,000 synthetic events per run with attributes like `event_id`, `user_id`, `product_id`, `event_type`, `event_time`, `price`, `category`, `user_device`, and `user_location`.
- **Streaming Processing**: Processes data in micro-batches of up to 1,000 records every 10 seconds using Spark Streaming.
- **Database Storage**: Writes processed events to PostgreSQL via JDBC with configurable batch sizes.
- **Scalability**: Designed to handle large datasets with checkpointing to prevent reprocessing and ensure fault tolerance.

## Directory Structure
- `notebooks/`: Contains `data_generator.py`, `spark_streaming_to_postgres.py`, and `postgres_connection.json`.
- `data/`: Stores generated CSV files (e.g., `user_events_YYYYMMDD_HHMMSS.csv`).
- `jars/`: Contains the PostgreSQL JDBC driver (`postgresql-42.7.3.jar`).
- `Dockerfile.pyspark`: Builds the PySpark notebook container with required dependencies.
- `docker-compose.yml`: Defines the services (Spark master, worker, PySpark notebook, and optionally PostgreSQL).

## Technologies Used
- **Apache Spark 3.5**: For streaming and processing.
- **PostgreSQL 13**: For persistent storage.
- **Docker**: For containerization and orchestration.
- **Python/PySpark**: For data generation and streaming logic.
- **pgAdmin**: For database management and visualization.

## Status
The pipeline is functional, generating 30,000 events per run and streaming them to PostgreSQL. Data is visible in pgAdmin, with ongoing optimization to prevent duplicate processing and improve logging.