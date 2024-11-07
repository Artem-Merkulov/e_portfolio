from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import pandas as pd
import logging

# Параметры подключения
POSTGRES_CONN_ID = 'postgres_local'
CSV_FILE_PATHS = [
    '/usr/local/airflow/dags/csv/user.csv',  # Путь к CSV файлам
    '/usr/local/airflow/dags/csv/group.csv',
    '/usr/local/airflow/dags/csv/group_log.csv',
    '/usr/local/airflow/dags/csv/dialog.csv'
]
TABLE_NAMES = ['user', 'group', 'group_log', 'dialog']
SQL_FILES = [
    '/usr/local/airflow/dags/sql/create_source_tables.sql',
    '/usr/local/airflow/dags/sql/create_source_references.sql'
]
        
def load_csv_to_postgres(table_name, csv_file_path, **kwargs):
    
    # Формируем строку подключения к PostgreSQL
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    # Формируем строку подключения к PostgreSQL
    conn_string = f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/postgres?options=-c%20search_path%3Dauto_dv'
    logging.info(f"Connection string: {conn}")

    if conn_string is None:
        logging.error("Failed to retrieve connection string from Airflow Connection.")
        return

    engine = create_engine(conn_string)
    
    # Загружаем данные из CSV в таблицу
    df = pd.read_csv(csv_file_path)
    df.to_sql(table_name, engine, if_exists='append', index=False)

with DAG(
    dag_id='autodv-init',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["autodv", "postgres", "init"]
) as dag:

    # Создание таблиц в PostgreSQL
    with open(SQL_FILES[0]) as f:
        create_source_tables = SQLExecuteQueryOperator(
            task_id='create_source_tables',
            sql=f.read(),
            conn_id=POSTGRES_CONN_ID,
            dag=dag,
        )

    # Загрузка данных из CSV в таблицы
    load_csv_user = PythonOperator(
        task_id='load_user',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': TABLE_NAMES[0], 'csv_file_path': CSV_FILE_PATHS[0]},
        dag=dag,
    )

    load_csv_group = PythonOperator(
        task_id='load_group',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': TABLE_NAMES[1], 'csv_file_path': CSV_FILE_PATHS[1]},
        dag=dag,
    )

    load_csv_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': TABLE_NAMES[2], 'csv_file_path': CSV_FILE_PATHS[2]},
        dag=dag,
    )
    
    load_csv_dialog = PythonOperator(
        task_id='load_dialog',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': TABLE_NAMES[3], 'csv_file_path': CSV_FILE_PATHS[3]},
        dag=dag,
    )

    # Создание внешних ключей в PostgreSQL
    with open(SQL_FILES[1]) as f:
        create_source_references = SQLExecuteQueryOperator(
            task_id='create_references',
            sql=f.read(),
            conn_id=POSTGRES_CONN_ID,
            dag=dag,
        )
    
    # Задаем порядок выполнения задач
    create_source_tables >> [load_csv_user, load_csv_group, load_csv_group_log, load_csv_dialog] >> create_source_references