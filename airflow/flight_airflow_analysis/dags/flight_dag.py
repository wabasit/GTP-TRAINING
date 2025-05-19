import sys
import os
sys.path.append('/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.csv_loader import load_csv_to_mysql
from scripts.data_validator import validate_data
from scripts.transformer import transform_and_compute_kpis
from scripts.postgres_loader import verify_postgres_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'flight_price_analysis',
    default_args=default_args,
    description='Flight Price Analysis Pipeline for Bangladesh',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 18),
    catchup=False,
) as dag:
    init_mysql = BashOperator(
        task_id='init_mysql',
        bash_command='mysql -h flight_mysql -u user -ppassword flight_staging_db < /opt/airflow/sql/mysql.sql'
    )

    init_postgres = BashOperator(
        task_id='init_postgres',
        bash_command='PGPASSWORD=postgres psql -h flight_postgres -U postgres -d flight_analytics_db -f /opt/airflow/sql/postgres.sql'
    )

    load_csv = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_mysql,
        op_kwargs={'csv_path': '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'}
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    transform_kpis = PythonOperator(
        task_id='transform_kpis',
        python_callable=transform_and_compute_kpis
    )

    verify_data = PythonOperator(
        task_id='verify_data',
        python_callable=verify_postgres_data
    )

    init_mysql >> init_postgres >> load_csv >> validate_data_task >> transform_kpis >> verify_data