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
pg_hook = BaseHook.get_connection('postgresql_conn')
vertica_hook = BaseHook.get_connection('vertica_conn')


# Записываю маршруты в переменные
select_currencies_path = '/lessons/select_currencies_from_pg.sql'
copy_currencies_path = '/lessons/copy_currencies_from_pg.sql'
delete_currencies_path = '/lessons/delete_currencies_from_pg.sql'
currencies_chunk_path = '/tmp/currencies_chunk.csv'

select_transactions_path = '/lessons/select_transactions_from_pg.sql'
copy_transactions_path = '/lessons/copy_transactions_from_pg.sql'
delete_transactions_path = '/lessons/delete_transactions_from_pg.sql'
transactions_chunk_path = '/tmp/transactions.csv'


# Чтение SQL-кода из файла и запись его в переменную
def add_date(path: str) -> str:

    sql_file = open(path, 'r')
    get_query = sql_file.read()
    get_query = get_query.replace(':business_date', f"'{business_date}'")
    sql_file.close()
    return get_query

# Чтение SQL-кода из файла и запись его в переменную
select_currencies_query = add_date(select_currencies_path)
copy_currencies_query = add_date(copy_currencies_path)
delete_currencies_query = add_date(delete_currencies_path)

select_transactions_query = add_date(select_transactions_path)
copy_transactions_query = add_date(copy_transactions_path)
delete_transactions_query = add_date(delete_transactions_path)


# Задаю функцию для чтнения из Postgres и записи в CSV-file.
def load_from_pg_to_csv(pg_hook: Connection,
                        business_date: str,
                        select_query: str,
                        chunk_path: str) -> None:
    
    with psycopg2.connect(
        host=pg_hook.host,
        dbname=pg_hook.schema,
        user=pg_hook.login,
        password=pg_hook.password,
        port=pg_hook.port
        ) as pg_conn:
    
        df = pd.read_sql_query(select_query, pg_conn)
        logger.info(f"Values by date {business_date} has been selected from Postgres!")
    
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
            
            
            
op_currencies_select_kwargs={
            'pg_hook': pg_hook,
            'business_date': business_date,
            'select_query': select_currencies_query,
            'chunk_path': currencies_chunk_path
           }

op_currencies_copy_kwargs={
            'vertica_hook': vertica_hook,
            'business_date': business_date,
            'delete_query': delete_currencies_query,
            'copy_query': copy_currencies_query,
            'chunk_path': currencies_chunk_path
           }

op_transactions_select_kwargs={
            'pg_hook': pg_hook,
            'business_date': business_date,
            'select_query': select_transactions_query,
            'chunk_path': transactions_chunk_path
           }

op_transactions_copy_kwargs={
            'vertica_hook': vertica_hook,
            'business_date': business_date,
            'delete_query': delete_transactions_query,
            'copy_query': copy_transactions_query,
            'chunk_path': transactions_chunk_path
           }



# Объявляю DAG 
default_args = {
    'owner': 'airflow',
    'description': 'From PG to Vertica DAG',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 1),
    'end_date': datetime(2022, 10, 31),
    'max_active_runs': 1,
    'max_consecutive_failed_dag_runs': 3, 
    'catchup': True,
    'tags': ['final_sprint', 'stg'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('pg_to_vrtc', default_args=default_args, schedule_interval='@daily', start_date = datetime(2022, 10, 1), end_date = datetime(2022, 10, 31))


start = EmptyOperator(task_id='start')


# Из PG в CSV
currencies_from_pg_to_csv = PythonOperator(
    task_id="currencies_from_pg_to_csv",
    op_kwargs=op_currencies_select_kwargs,
    python_callable=load_from_pg_to_csv,
    dag=dag
)


# Из CSV в Vertica
currencies_from_csv_to_vertica = PythonOperator(
    task_id="currencies_from_csv_to_vertica",
    op_kwargs=op_currencies_copy_kwargs,
    python_callable=load_from_csv_to_vertica,
    dag=dag
)


# Из PG в CSV
transactions_from_pg_to_csv = PythonOperator(
    task_id="transactions_from_pg_to_csv",
    op_kwargs=op_transactions_select_kwargs,
    python_callable=load_from_pg_to_csv,
    dag=dag
)


# Из CSV в Vertica
transactions_from_csv_to_vertica = PythonOperator(
    task_id="transactions_from_csv_to_vertica",
    op_kwargs=op_transactions_copy_kwargs,
    python_callable=load_from_csv_to_vertica,
    dag=dag
)



end = EmptyOperator(task_id='end')


start >> currencies_from_pg_to_csv >> currencies_from_csv_to_vertica >> transactions_from_pg_to_csv >> transactions_from_csv_to_vertica >> end