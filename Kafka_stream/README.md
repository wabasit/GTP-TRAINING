# Heartbeat Monitoring Pipeline

----

This project implements a real-time heartbeat monitoring pipeline that generates random heart rate data, streams it to a Kafka topic, processes it to detect anomalies, stores it in a PostgreSQL database, and visualizes it using Grafana. The system is containerized using Docker Compose for easy deployment and scalability.

## Overview

- **Producer**: Generates random heart rate data and sends it to a Kafka topic.
- **Kafka**: Acts as a message broker to stream the data.
- **Consumer**: Consumes the data, detects anomalies (heart rate < 50 or > 140 bpm), and inserts it into PostgreSQL.
- **PostgreSQL**: Stores the heartbeat data with timestamps and anomaly flags.
- **Grafana**: Provides visualizations of heart rate trends and anomalies over time.

## Prerequisites

- **Docker**: Ensure Docker and Docker Compose are installed.
  - [Install Docker](https://docs.docker.com/get-docker/)
  - [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Windows, macOS, or Linux**: Tested on Windows (e.g., your setup at `C:\Users\dell\Desktop\Python Projects\AmaliTech\Labs\Kafka_stream`).
- **Python Knowledge**: Basic understanding for script modifications.

## Project Structure
### Kafka_stream/
        ├── data_generator.py        # Script to generate and send heartbeat data to Kafka
        ├── consumer.py             # Script to consume data, detect anomalies, and store in PostgreSQL
        ├── docker-compose.yml      # Defines the Docker services 
        ├── README.md               # This file


## Setup Instructions

1. **Clone or Set Up the Repository**:
   - Create a directory (e.g., `C:\Users\dell\Desktop\Python Projects\AmaliTech\Labs\Kafka_stream`) and place the files (`data_generator.py`, `consumer.py`, `docker-compose.yml`) inside it.

2. **Install Dependencies**:
   - Docker and Docker Compose should be pre-installed. No additional Python dependencies are needed outside the containers.

3. **Start the Pipeline**:
   - Open a terminal in the `Kafka_stream` directory and run:
     ```bash
     docker-compose up -d
     ```

4. **View logs to ensure services are running**:
    - docker logs kafka_stream-producer-1
    - docker logs kafka_stream-consumer-1
    - docker logs kafka_stream-kafka-1
    - docker logs kafka_stream-postgres-1
    - docker logs kafka_stream-grafana-1

5. **Confirm data in PostgreSQL**:
```bash
docker exec -it kafka_stream-postgres-1 psql -U postgres -d heartbeats_db -c "SELECT * FROM heartbeats ORDER BY timestamp DESC LIMIT 10;"
```

## Access Grafana:
    Open your browser and go to http://localhost:3000.
    Log in with username admin and password admin.
    Configure the PostgreSQL data source and create visualizations (see Usage).

### Visualizations in Grafana

**Add PostgreSQL Data Source**:
    ***Name***: heartbeats_db
    ***Host***: postgres:5432
    ***Database***: heartbeats_db
    ***User***: postgres
    ***Password***: postgres
    ***TLS/SSL Mode***: disable

**Create Dashboard**:
    Add a new dashboard and create panels with these queries

## Architecture
### System Architecture
    The pipeline follows a microservices architecture with the following components:

- **Data Generator (Producer)**:
    - ***Language***: Python
    - ***Role***: Generates random heart rate data (customer_id, timestamp, heart_rate) and sends it to a Kafka topic (heart_rate_stream) every 1 second (configurable).
    - ***Technology***: Confluent Kafka Python client.
    - ***Deployment***: Docker container.

- **Kafka**:
    - ***Role***: Message broker that queues the heartbeat data.
    - ***Technology***: Apache Kafka with Zookeeper for coordination.
    - ***Configuration***: Listens on port 9092, advertises kafka:9092.
    - ***Deployment***: Docker container.

- **Consumer**:
    - ***Language***: Python
    - ***Role***: Consumes data from the Kafka topic, detects anomalies (heart rate < 50 or > 140 bpm), and inserts into PostgreSQL in batches of 10.
    - ***Technology***: Confluent Kafka and psycopg2 Python clients.
    - ***Deployment***: Docker container.

- **PostgreSQL**:
    - ***Role***: Persistent storage for heartbeat data.
    - ***Schema***: heartbeats table with TIMESTAMPTZ for time zone support.
    - ***Configuration***: Runs on port 5432, database heartbeats_db.
    - ***Deployment***: Docker container with persistent volume.

- **Grafana**:
    - ***Role***: Visualization layer for real-time monitoring.
    - ***Configuration***: Runs on port 3000, connects to PostgreSQL.
    - ***Deployment***: Docker container with persistent storage.

### Data Flow
    The producer generates random heartbeat data and publishes it to the heart_rate_stream topic in Kafka.
    The consumer subscribes to the topic, processes the data (anomaly detection), and inserts it into the heartbeats table in PostgreSQL.
    Grafana queries the PostgreSQL database to display time-series charts, anomaly counts, and other visualizations.

### Diagram
[Producer] --> [Kafka Topic: heart_rate_stream] --> [Consumer] --> [PostgreSQL: heartbeats table] --> [Grafana Visualizations]
   |                                                                       |
   +-------------------------<---------------------------------------------+
### Troubleshooting

**Producer Connection Refused**:
    Check Kafka logs (docker logs kafka_stream-kafka-1).
    Increase sleep duration in docker-compose.yml (e.g., to 20 seconds).

**Consumer Errors**:
    Verify timestamp column is TIMESTAMPTZ in PostgreSQL.
    Check consumer.py logs for datatype mismatches.

**Grafana No Data**:
    Ensure the PostgreSQL data source is correctly configured.
    Confirm data exists in the heartbeats table.
    Contributing
    Feel free to fork this repository, submit issues, or pull requests for enhancements (e.g., adding more anomaly detection rules or visualizations).

---

## Architecture Description

#### System Architecture
    The Heartbeat Monitoring Pipeline is designed as a distributed system using a microservices approach, with each component running in its own Docker container orchestrated by Docker Compose. The architecture ensures scalability, fault tolerance, and real-time data processing.

- **Data Generator (Producer)**:
    - **Functionality**: Written in Python, it generates synthetic heartbeat data with random `customer_id` (1000-9999), `timestamp` (Unix epoch integer), and `heart_rate` (60-120 bpm). It sends this data to a Kafka topic every configurable interval (default 1 second).
    - **Technology**: Uses the Confluent Kafka Python client to produce messages.
    - **Deployment**: Runs in a `python:3.9-slim` Docker container, with the script mounted as a volume.

- **Kafka**:
    - **Functionality**: Serves as a distributed streaming platform to handle the heartbeat data. It uses Zookeeper for coordination and maintains the `heart_rate_stream` topic.
    - **Configuration**: Exposes port 9092, with `KAFKA_ADVERTISED_LISTENERS` set to `PLAINTEXT://kafka:9092` for Docker network resolution.
    - **Deployment**: Runs in a `confluentinc/cp-kafka:latest` container, dependent on a `confluentinc/cp-zookeeper:latest` container.

- **Consumer**:
    - **Functionality**: Written in Python, it subscribes to the `heart_rate_stream` topic, processes each message to detect anomalies (heart rate < 50 or > 140 bpm), and batches inserts (up to 10 records) into PostgreSQL.
    - **Technology**: Uses Confluent Kafka for consumption and `psycopg2` with `pytz` for PostgreSQL interaction and time zone handling.
    - **Deployment**: Runs in a `python:3.9-slim` container, with the script mounted as a volume.

- **PostgreSQL**:
    - **Functionality**: Stores the processed heartbeat data in the `heartbeats` table, which includes `id`, `customer_id`, `timestamp` (as `TIMESTAMPTZ`), `heart_rate`, and `anomaly` columns.
    - **Configuration**: Runs on port 5432, with a persistent volume for data durability.
    - **Deployment**: Uses `postgres:13` with environment variables for user, password, and database setup.

- **Grafana**:
    - **Functionality**: Provides a web-based interface for visualizing the heartbeat data. It connects to PostgreSQL to display time-series charts, anomaly counts, and other metrics.
    - **Configuration**: Runs on port 3000, with a persistent volume for dashboards and an admin user (`admin`/`admin`).
    - **Deployment**: Uses `grafana/grafana:latest` with a dependency on PostgreSQL.

#### Data Flow
 - **Data Generation**: The `producer` creates random heartbeat records and publishes them to the `heart_rate_stream` topic in Kafka.
 - **Message Queuing**: Kafka buffers the messages, ensuring reliable delivery to the `consumer`.
 - **Data Processing**: The `consumer` retrieves messages, applies anomaly detection logic, and inserts the data into PostgreSQL in batches.
 - **Visualization**: Grafana queries the `heartbeats` table to render real-time visualizations, updated every few seconds.

#### Diagram
[Producer (Python)] --> [Kafka (with Zookeeper)] --> [Consumer (Python)] --> [PostgreSQL] --> [Grafana]
|                                                                       |
+-------------------------<---------------------------------------------+

- **Arrows**: Represent the flow of data from generation to visualization.
- **Feedback Loop**: Grafana can be used to monitor and adjust the system if needed.

#### Scalability and Fault Tolerance
- **Scalability**: Adding more `producer` or `consumer` instances is possible by scaling the Docker services with `docker-compose up -d --scale producer=3`.
- **Fault Tolerance**: Kafka ensures message durability, PostgreSQL uses a persistent volume, and Docker’s `restart: unless-stopped` policy restarts failed containers.

---

