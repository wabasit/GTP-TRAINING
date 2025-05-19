# Flight Price Analysis Pipeline – Technical Report

## Overview

The **Flight Price Analysis Pipeline** is a fully containerized data engineering workflow designed to ingest, validate, transform, and analyze flight pricing data for Bangladesh. The system uses **Apache Airflow** (v2.10.2) for orchestration and runs in a **Docker-based environment** for easy deployment and portability.

---

## 1. Pipeline Architecture and Execution Flow

### Architecture Summary

The system is composed of the following services defined in `docker-compose.yml`:

| Service                    | Description                                                   |
| -------------------------- | ------------------------------------------------------------- |
| `flight_postgres`          | PostgreSQL 13 – Hosts analytics database and Airflow metadata |
| `flight_mysql`             | MySQL 8.0 – Hosts the staging (raw) data                      |
| `flight_airflow-webserver` | Airflow web UI container (port `8080`)                        |
| `flight_airflow-scheduler` | Executes DAGs (scheduler service)                             |

* **Persistent Volumes**: `postgres_data` and `mysql_data`
* **Custom Dockerfile** adds:

  * `mysql-client`, `postgresql-client`
  * Python dependencies: `pandas`, `mysql-connector-python`, `psycopg2-binary`, `python-dotenv`
* **Network**: `airflow-network` (bridge)

### Data Flow

```
CSV → MySQL Staging → Validation → PostgreSQL Analytics → KPI Computation
```

1. **Input**: `Flight_Price_Dataset_of_Bangladesh.csv` (in `./data/`)
2. **Staging**: `raw_flight_prices` in MySQL
3. **Validation**: Detects nulls, negative fares
4. **Transformation**: Clean data into `analytics.flight_prices`
5. **KPI Computation**: Average fare per airline → `analytics.kpi_metrics`

### DAG Execution Flow

**DAG ID**: `flight_price_analysis`
**Schedule**: Daily (`@daily`)
**Start Date**: May 18, 2025
**Catchup**: ❌ Disabled
**Retries**: 1 (after 5 minutes)
**Logs Directory** (host): `./logs` → mapped to `/opt/airflow/scripts/logs/`

#### Task Sequence:

```
init_mysql ➝ init_postgres ➝ load_csv ➝ validate_data ➝ transform_kpis ➝ verify_data
```

---

## 2. DAG and Task Descriptions

### DAG: `flight_price_analysis`

| Field             | Value                              |
| ----------------- | ---------------------------------- |
| Schedule Interval | `timedelta(days=1)`                |
| Start Date        | `datetime(2025, 5, 18)`             |
| Retries           | 1                                  |
| Retry Delay       | `timedelta(minutes=5)`             |
| Description       | End-to-end flight pricing workflow |

### Task Breakdown

#### 1. `init_mysql` (BashOperator)

* **Purpose**: Initializes `raw_flight_prices` table
* **SQL File**: `/opt/airflow/sql/mysql.sql`
* **Command**:

  ```bash
  mysql -h flight_mysql -u user -ppassword flight_staging_db < /opt/airflow/sql/mysql.sql
  ```

#### 2. `init_postgres` (BashOperator)

* **Purpose**: Initializes analytics tables in PostgreSQL
* **SQL File**: `/opt/airflow/sql/postgres.sql`
* **Command**:

  ```bash
  psql -h flight_postgres -U postgres -d flight_analytics_db -f /opt/airflow/sql/postgres.sql
  ```

#### 3. `load_csv` (PythonOperator)

* **Function**: `load_csv_to_mysql` (in `scripts/csv_loader.py`)
* **Key Steps**:

  * Loads CSV via `pandas`
  * Inserts rows using `mysql-connector-python`
* **Logs**: `/opt/airflow/scripts/logs/csv_loader.log` (mapped to `./logs/csv_loader.log`)

#### 4. `validate_data` (PythonOperator)

* **Function**: `validate_data` (in `scripts/data_validator.py`)
* **Logic**:

  * Sets `is_valid=FALSE` for:

    * NULLs in `airline`, `source`, `destination`
    * Negative/zero `base_fare`, `total_fare`
* **Logs**: `data_validator.log`

#### 5. `transform_kpis` (PythonOperator)

* **Function**: `transform_and_compute_kpis` (in `scripts/transformer.py`)
* **Steps**:

  * Transfers valid rows to PostgreSQL
  * Computes **Average Fare per Airline**
* **Logs**: `transformer.log`

#### 6. `verify_data` (PythonOperator)

* **Function**: `verify_postgres_data` (in `scripts/postgres_loader.py`)
* **Purpose**: Verifies row counts in PostgreSQL tables
* **Logs**: `postgres_loader.log`

---

## 3. KPI Metrics and Logic

### KPI Computed

| KPI Name   | Description                               | Table                   |
| ---------- | ----------------------------------------- | ----------------------- |
| `avg_fare` | Average `total_fare` grouped by `airline` | `analytics.kpi_metrics` |

### Computation Logic

1. **SQL**:

```sql
SELECT airline, AVG(total_fare) as avg_fare 
FROM raw_flight_prices 
WHERE is_valid = TRUE 
GROUP BY airline;
```

2. **Insert into PostgreSQL**:

```sql
INSERT INTO analytics.kpi_metrics (metric_name, metric_value, airline, calculation_date)
VALUES ('avg_fare', 6000.0, 'Biman', CURRENT_DATE);
```

3. **Sample Output**:
   \| metric\_name | metric\_value | airline   | calculation\_date |
   \|-------------|--------------|-----------|------------------|
   \| avg\_fare    | 6000.00      | Biman     | 2025-05-18        |
   \| avg\_fare    | 5400.00      | US-Bangla | 2025-05-18        |

---

## 4. Logging Strategy

### Updated Log Setup:

* **Directory**: All logs are saved inside the host machine at:
  `./logs/` → mapped to `/opt/airflow/scripts/logs/` in containers.
* **Log Files**:

  * `csv_loader.log`
  * `data_validator.log`
  * `transformer.log`
  * `postgres_loader.log`

### Logging Enhancements:

* Used `RotatingFileHandler` to prevent log bloating
* Included timestamps and log levels
* Standardized format:

```python
'%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

---

## 5. Challenges Encountered and Resolutions

### Challenge 1: Airflow Containers Crashing
- **Description**: The `flight_airflow-webserver` and `flight_airflow-scheduler` containers exited with status `Exited (1)` due to the missing `airflow` database.
- **Error Logs**:
  - `FATAL: database "airflow" does not exist`.
  - `ERROR: You need to initialize the database. Please run 'airflow db init'`.
- **Resolution**:
  - Created the `airflow` database:
    ```powershell
    docker exec -it flight_postgres psql -U postgres -c "CREATE DATABASE airflow;"
    ```
  - Initialized the database using a temporary container:
    ```powershell
    docker run -it --rm --network flight_airflow_analysis_airflow-network -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@flight_postgres:5432/airflow apache/airflow:2.10.2 airflow db init
    ```
  - Created an admin user:
    ```powershell
    docker run -it --rm --network flight_airflow_analysis_airflow-network -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@flight_postgres:5432/airflow apache/airflow:2.10.2 airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
    ```
  - Rebuilt and restarted containers:
    ```powershell
    docker-compose down
    docker-compose build
    docker-compose up -d
    ```
- **Outcome**: Containers started successfully, and the Airflow UI was accessible at `http://localhost:8080`.

### Challenge 2: ModuleNotFoundError for Scripts
- **Description**: The DAG failed with `ModuleNotFoundError: No module named 'scripts.csv_loader'` when importing `load_csv_to_mysql` from `scripts/csv_loader.py`.
- **Cause**: The `scripts` directory was not in the Python path, and `scripts` was not recognized as a Python package.
- **Resolution**:
  - Added `sys.path.append('/opt/airflow/scripts')` to `flight_dag.py` to include the `scripts` directory in the Python path.
  - Created an empty `__init__.py` in `scripts/` to make it a Python package:
    ```powershell
    echo $null > C:\Users\dell\Desktop\Python\Projects\AmaliTech\Labs\airflow\flight_airflow_analysis\scripts\__init__.py
    ```
  - Verified file presence:
    ```powershell
    docker exec -it flight_airflow-webserver ls /opt/airflow/scripts
    ```
  - Ensured `docker-compose.yml` correctly mounted `./scripts:/opt/airflow/scripts`.
- **Outcome**: The DAG loaded without import errors.

### Challenge 3: Incorrect MySQL Host in `csv_loader.py`
- **Description**: The `load_csv` task could fail because `csv_loader.py` used `host="mysql"` instead of `host="flight_mysql"`, as defined in `docker-compose.yml`.
- **Resolution**:
  - Updated `csv_loader.py` to use `host="flight_mysql"`:
    ```python
    conn = mysql.connector.connect(
        host="flight_mysql",
        user="user",
        password="password",
        database="flight_staging_db"
    )
    ```
  - Removed unnecessary `load_dotenv()` since credentials were hardcoded in `docker-compose.yml`.
  - Tested the connection:
    ```powershell
    docker exec -it flight_airflow-webserver bash -c "mysql -h flight_mysql -u user -ppassword flight_staging_db -e 'SELECT 1;'"
    ```
- **Outcome**: Ensured connectivity to the MySQL database.

### Challenge 4: Mismatched CSV Column Names
- **Description**: The `load_csv` task could fail if the CSV column names in `Flight_Price_Dataset_of_Bangladesh.csv` didn’t match those expected in `csv_loader.py` (e.g., `Airline` vs. `airline`).
- **Resolution**:
  - Updated `csv_loader.py` to use lowercase column names (`airline`, `source`, etc.) to match the provided CSV:
    ```python
    cursor.execute(insert_query, (
        row.get('airline'), row.get('source'), row.get('destination'),
        row.get('base_fare'), row.get('tax_surcharge'), row.get('total_fare'),
        row.get('booking_date'), row.get('travel_date')
    ))
    ```
  - Verified the CSV format:
    ```csv
    airline,source,destination,base_fare,tax_surcharge,total_fare,booking_date,travel_date
    Biman,B Dhaka,B Chittagong,5000.00,1000.00,6000.00,2025-01-01,2025-02-01
    US-Bangla,B Dhaka,B Sylhet,4500.00,900.00,5400.00,2025-01-02,2025-02-02
    ```
- **Outcome**: Ensured data was correctly loaded into `raw_flight_prices`.

### Challenge 5: Missing Dependencies and Clients
- **Description**: Potential failures in `init_mysql` and `init_postgres` tasks if `mysql-client` or `postgresql-client` were not installed in the Airflow containers.
- **Resolution**:
  - Updated `Dockerfile` to include:
    ```dockerfile
    RUN apt-get update && apt-get install -y \
        default-mysql-client \
        postgresql-client \
        && apt-get clean
    ```
  - Ensured `requirements.txt` included:
    ```
    pandas
    mysql-connector-python
    psycopg2-binary
    python-dotenv
    ```
  - Rebuilt containers:
    ```powershell
    docker-compose build
    ```
  - Verified clients:
    ```powershell
    docker exec -it flight_airflow-webserver bash -c "mysql --version && psql --version"
    ```
- **Outcome**: Tasks executed SQL scripts successfully.
### Debugging & Fixes Summary

| Issue                             | Root Cause                             | Solution/Debug Steps                           |
| --------------------------------- | -------------------------------------- | ---------------------------------------------- |
| `mysql.connector` connection fail | Missing client in Dockerfile           | Added `mysql-client`, rebuilt image            |
| Logs not showing on host          | Improper mount or missing log folder   | Created `./logs`, updated `volumes` in Compose |
| Empty analytics table             | Validation marking all rows as invalid | Added debug prints, improved validation rules  |
| `psycopg2` connection fail        | Wrong host name in DAG                 | Corrected to `flight_postgres`                 |

---

## 6. Summary
The **Flight Price Analysis Pipeline** successfully automates the processing of flight price data using Airflow, MySQL, and PostgreSQL in a Dockerized environment. The pipeline architecture ensures modularity and scalability, with clear separation of staging (MySQL) and analytics (PostgreSQL) data. The `flight_price_analysis` DAG via the Airflow UI (`http://localhost:8080`) orchestrates six tasks to create tables, load, validate, transform, and verify data, computing the average fare per airline as the primary KPI. Challenges such as container crashes, module import errors, and configuration mismatches were resolved through database initialization, Python path adjustments, and configuration updates. The pipeline is now ready for production use, with recommendations for adding more KPIs, securing credentials, and scaling resources.

---
 