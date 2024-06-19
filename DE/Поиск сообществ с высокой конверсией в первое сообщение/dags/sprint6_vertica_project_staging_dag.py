from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import vertica_python

import boto3
import pendulum


AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"\

# Задаю параметры подключения
conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv202310168',       
             'password': 'vxsf5Lq0dYYinqy',
             'database': 'dwh',
             'autocommit': True
       }
# Сохраняю имя схемы staging в Vertica в переменную 
staging_schema = 'STV202310168__STAGING'

# Задаю функцию для загрузки csv файлов из S3 в хранилище docker
def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
				Bucket=bucket, 
				Key=key, 
				Filename=f'/data/{key}'
)

# Переменная для вывода 10 строк
bash_command_tmpl = """
head {{ params.files }}
"""

# Задаю функцию для создания таблиц в слое staging
def create_staging_tables():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()

        # Delete tables if exist
        cur.execute(f"DROP TABLE IF EXISTS {staging_schema}.users CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {staging_schema}.groups CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {staging_schema}.dialogs CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {staging_schema}.group_log CASCADE")

        # Create users table              в поле registration_dt есть нулевые значения, поэтому по нему не получается партиционировать
        cur.execute(f"""
            CREATE TABLE {staging_schema}.users(
                id              int     PRIMARY KEY,
                chat_name       varchar(200)       ,
                registration_dt timestamp          ,
                country         varchar(200)       ,
                age             int         
            )
            ORDER BY id
            SEGMENTED BY id all nodes;
        """)
        
        # Create groups table
        cur.execute(f"""
            CREATE TABLE STV202310168__STAGING.groups(
                id	            int     PRIMARY KEY,
                admin_id	    int                ,
                group_name	    varchar(100)       ,
                registration_dt	datetime           ,
                is_private      int            
            )
            order by id, admin_id
            SEGMENTED BY id all nodes
            PARTITION BY registration_dt::date
            GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);

            ALTER TABLE STV202310168__STAGING.groups ADD FOREIGN KEY (admin_id)      REFERENCES STV202310168__STAGING.users(id);                
        """)
        
        # Create dialogs table
        cur.execute(f"""
            CREATE TABLE STV202310168__STAGING.dialogs(
                message_id	    int     PRIMARY KEY,
                message_ts	    datetime           ,
                message_from	int                ,
                message_to	    int                ,
                message	        varchar(1000)      ,
                message_group   float             
            )
            ORDER BY message_id
            SEGMENTED BY message_id all nodes
            PARTITION BY message_ts::date
            GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

            ALTER TABLE STV202310168__STAGING.dialogs ADD FOREIGN KEY (message_from) REFERENCES STV202310168__STAGING.users(id);
            ALTER TABLE STV202310168__STAGING.dialogs ADD FOREIGN KEY (message_to)   REFERENCES STV202310168__STAGING.users(id);
        """)

        # Create group_log table
        cur.execute(f"""
            CREATE TABLE STV202310168__STAGING.group_log(
                group_id	    int     PRIMARY KEY,
                user_id	        int                ,
                user_id_from	float              ,
                event	        varchar(100)       ,
                "datetime"      datetime           
            )
            ORDER BY group_id
            SEGMENTED BY group_id all nodes
            PARTITION BY "datetime"::date
            GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);

            ALTER TABLE STV202310168__STAGING.group_log ADD FOREIGN KEY (group_id)  REFERENCES STV202310168__STAGING.groups(id);
            ALTER TABLE STV202310168__STAGING.group_log ADD FOREIGN KEY (user_id)    REFERENCES STV202310168__STAGING.users(id);
        """)

def load_from_local_to_staging_tables():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        # Load users data
        cur.execute(f"""
            COPY {staging_schema}.users 
            FROM LOCAL '/data/users.csv'
           DELIMITER ','
           DIRECT
        """)
        
        # Load groups data
        cur.execute(f"""
            COPY {staging_schema}.groups
            FROM LOCAL '/data/groups.csv' 
            DELIMITER ','
            DIRECT
        """)   
        
        # Load dialogs data
        cur.execute(f"""
            COPY {staging_schema}.dialogs
            FROM LOCAL '/data/dialogs.csv'
            DELIMITER ','
            DIRECT
        """)

        # Load group_log data
        cur.execute(f"""
            COPY {staging_schema}.group_log
            FROM LOCAL '/data/group_log.csv'
            DELIMITER ','
            DIRECT
        """)

@dag(dag_id='sprint6_vertica_project_staging_dag',
     schedule_interval='@daily',  
     catchup=False,
     tags=['sprint6', '1_dag', 'staging', 'from_s3_to_stg', 'ddl', 'etl'],
     is_paused_upon_creation=True,
     start_date=pendulum.parse('2022-07-13'))

# Задаю функцию для загрузки csv файлов из хранилища S3 в docker
def sprint6_project_dag_get_data():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv' )
    download_from_S3 = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]
        
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )

    create_stg_tables = PythonOperator(
        task_id='create_tables_in_staging',
        python_callable=create_staging_tables
    )
  
    load_to_stg_tables = PythonOperator(
        task_id='load_from_local_to_staging_tables',
        python_callable=load_from_local_to_staging_tables
    )


    download_from_S3 >> print_10_lines_of_each >> create_stg_tables >> load_to_stg_tables

_ = sprint6_project_dag_get_data()


