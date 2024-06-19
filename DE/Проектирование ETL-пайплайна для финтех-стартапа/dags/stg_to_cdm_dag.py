# Импорты

# Стандартные библиотеки Python
import logging
import psycopg2
import pandas as pd
import vertica_python
from typing import List, Dict
from datetime import datetime, timedelta

# модули AirFlow
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection


# Задаю параметры логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__) 


# Прописываю Jinja Templates с датой выполнения дага
business_date = r'{{ ds }}'

# Импортирую параметры подключений из AirFlow Connections
vertica_hook = BaseHook.get_connection('vertica_conn')


# Записываю маршруты в переменные
select_global_metrics_path = '/lessons/select_global_metrics_from_stg.sql'
copy_global_metrics_path = '/lessons/copy_global_metrics_to_cdm.sql'
delete_global_metrics_path = '/lessons/delete_global_metrics.sql'
global_metrics_chunk_path = '/tmp/global_metrics_chunk.csv'


# Чтение SQL-кода из файла и запись его в переменную
def add_date(path: str) -> str:

    sql_file = open(path, 'r')
    get_query = sql_file.read()
    get_query = get_query.replace(':business_date', f"'{business_date}'")
    sql_file.close()
    return get_query

# Чтение SQL-кода из файла и запись его в переменную
select_global_metrics_query = add_date(select_global_metrics_path)
copy_global_metrics_query = add_date(copy_global_metrics_path)
delete_global_metrics_query = add_date(delete_global_metrics_path)


# Задаю функцию для чтнения из Postgres и записи в CSV-file.
def load_from_stg_to_csv(vertica_hook: Connection,
                         business_date: str,
                         select_query: str,
                         chunk_path: str) -> None:
    
    # Подключение к Vertica
    with vertica_python.connect(
        host=vertica_hook.host,
        user=vertica_hook.login,
        password=vertica_hook.password,
        port=vertica_hook.port,
        database=vertica_hook.schema
    ) as vertica_conn:
    
        df = pd.read_sql_query(select_query, vertica_conn)
        logger.info(f"Values by date {business_date} has been selected from staging!")
    
    # Записываю df в csv-файл и сохраняю
    df.to_csv(chunk_path, index=False)
    logger.info(f"Values by date {business_date} has been saved to csv-file!")
    
    
def load_from_csv_to_vertica(vertica_hook: Connection,
                             business_date: str,
                             delete_query: str,
                             copy_query: str,
                             chunk_path: str) -> None:
        
    # Подключение к Vertica
    with vertica_python.connect(
        host=vertica_hook.host,
        user=vertica_hook.login,
        password=vertica_hook.password,
        port=vertica_hook.port,
        database=vertica_hook.schema
    ) as vertica_conn:
        
        try:
            
            with vertica_conn.cursor() as cur:
        
                cur.execute(delete_query)
        
                with open(chunk_path, 'rb') as chunk:
                    cur.copy(copy_query, chunk)
            
                vertica_conn.commit()
                logger.info(f"Values by date {business_date} has been updated!")
            
        except Exception as e:
                logger.error(f"Values by date {business_date} has not been updated!")
                vertica_conn.rollback()
                raise e
            
        else:
            vertica_conn.commit() 
            
            
op_global_metrics_select_kwargs={
            'vertica_hook': vertica_hook,
            'business_date': business_date,
            'select_query': select_global_metrics_query,
            'chunk_path': global_metrics_chunk_path
           }

op_global_metrics_copy_kwargs={
            'vertica_hook': vertica_hook,
            'business_date': business_date,
            'delete_query': delete_global_metrics_query,
            'copy_query': copy_global_metrics_query,
            'chunk_path': global_metrics_chunk_path
           }


# Объявляю DAG 
default_args = {
    'owner': 'airflow',
    'description': 'From stg to cdm Vertica DAG',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'max_active_runs': 1,
    'max_consecutive_failed_dag_runs': 3, 
    'catchup': True,
    'tags': ['final_sprint', 'cdm'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('stg_to_cdm', default_args=default_args, schedule_interval='@daily', start_date = datetime(2022, 10, 1), end_date = datetime(2022, 10, 31))


start = EmptyOperator(task_id='start')

# Из STG в CSV
global_metrics_from_stg_to_csv = PythonOperator(
    task_id="global_metrics_from_stg_to_csv",
    op_kwargs=op_global_metrics_select_kwargs,
    python_callable=load_from_stg_to_csv,
    dag=dag
)


# Из CSV в CDM
global_metrics_from_csv_to_cdm = PythonOperator(
    task_id="global_metrics_from_csv_to_cdm",
    op_kwargs=op_global_metrics_copy_kwargs,
    python_callable=load_from_csv_to_vertica,
    dag=dag
)

end = EmptyOperator(task_id='end')

start >> global_metrics_from_stg_to_csv >> global_metrics_from_csv_to_cdm >> end