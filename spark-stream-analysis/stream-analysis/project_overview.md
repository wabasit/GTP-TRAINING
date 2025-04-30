# Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

## Components
- **Data Generator**: Creates fake e-commerce user events as CSVs.
- **Spark Streaming**: Monitors the data folder, reads incoming files, applies transformations, and writes to PostgreSQL.
- **PostgreSQL Database**: Stores structured event data for querying and analytics.

## Flow
1. `data_generator.py` creates event CSVs in `/data/`.
2. Spark monitors `/data/` and processes files as they arrive.
3. Cleaned events are inserted into `user_events` table in PostgreSQL.

## Key Technologies
- Apache Spark Structured Streaming
- PostgreSQL
- Python (Faker, csv, uuid, etc.)
- Docker & Docker Compose
