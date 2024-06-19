import vertica_python
import pendulum
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

conn_info = {'host': 'vertica.tgcloudenv.ru', 
           'port': '5433',
           'user': 'stv202310168',       
           'password': 'vxsf5Lq0dYYinqy',
           'database': 'dwh',
           'autocommit': True
       }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2)
}

dwh_schema = 'STV202310168__DWH'


# Задаю функцию для создания таблиц слоя dds
def create_dwh_tables():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()

        # Delete tables if exist

        # hubs
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.h_users CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.h_groups CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.h_dialogs CASCADE")

        # project6 hubs
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.h_group_log CASCADE") # добавил group_log

        # links
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.l_admins CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.l_groups_dialogs CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.l_user_message CASCADE")

        # project6 links
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.l_user_group_activity CASCADE")

        # satellites 
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_admins CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_dialog_info CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_group_name CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_group_private_status CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_user_chatinfo CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_user_socdem CASCADE")

        # project6 satellites 
        cur.execute(f"DROP TABLE IF EXISTS {dwh_schema}.s_auth_history CASCADE")

        # create tables

        # hubs

        # Create h_users
        cur.execute(f"""
            create table STV202310168__DWH.h_users
            (
                hk_user_id bigint primary key,
                user_id      int,
                registration_dt datetime,
                load_dt datetime,
                load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_user_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 
        """)

        # Create h_groups table
        cur.execute(f"""
            create table STV202310168__DWH.h_groups
            (
                hk_group_id bigint primary key,
                group_id      int,
                registration_dt datetime,
                load_dt datetime,
                load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_group_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);      
        """)

        # Create h_dialogs table
        cur.execute(f"""
            create table STV202310168__DWH.h_dialogs
            (
                hk_message_id bigint primary key,
                message_id      int,
                message_ts datetime,
                load_dt datetime,
                load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_message_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)

        # Create h_group_log table project6                      # добавил group_log
        cur.execute(f"""
            create table STV202310168__DWH.h_group_log           
            (
                hk_group_id bigint primary key,
                group_id     int,
                event_dt datetime,
                load_dt datetime,
                load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_group_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 
        """)


        # links

        # Create l_admins
        cur.execute(f"""
            create table STV202310168__DWH.l_admins
            (
            hk_l_admin_id bigint primary key,
            hk_user_id      bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV202310168__DWH.h_users (hk_user_id),
            hk_group_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202310168__DWH.h_groups (hk_group_id),
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_l_admin_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 
        """)

        # Create l_groups_dialogs
        cur.execute(f"""
            create table STV202310168__DWH.l_groups_dialogs
            (
            hk_l_groups_dialogs bigint primary key,
            hk_message_id      bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202310168__DWH.h_dialogs (hk_message_id),
            hk_group_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202310168__DWH.h_groups (hk_group_id),
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_l_groups_dialogs all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 
        """)

        # Create l_user_message
        cur.execute(f"""
            create table STV202310168__DWH.l_user_message
            (
            hk_l_user_message bigint primary key,
            hk_user_id      bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202310168__DWH.h_users (hk_user_id),
            hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202310168__DWH.h_dialogs (hk_message_id),
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_user_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 
        """)

        # project6 links

        # Create l_user_group_activity
        cur.execute(f"""
            CREATE TABLE STV202310168__DWH.l_user_group_activity
                        (
                        hk_l_user_group_activity int NOT NULL,
                        hk_user_id int NOT NULL,
                        hk_group_id int NOT NULL,
                        load_dt timestamp,
                        load_src varchar(20),
                        CONSTRAINT C_PRIMARY PRIMARY KEY (hk_l_user_group_activity) DISABLED
                        )
            ORDER BY
            load_dt
                        SEGMENTED BY hk_l_user_group_activity ALL nodes
                        PARTITION BY load_dt::date
            GROUP BY
            calendar_hierarchy_day(load_dt::date,
            3,
            2);

        """)

        # satellites

        # Create s_admins
        cur.execute(f"""
            create table STV202310168__DWH.s_admins
            (
            hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV202310168__DWH.l_admins (hk_l_admin_id),
            is_admin boolean,
            admin_from datetime,
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_admin_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)

        # Create s_dialog_info
        cur.execute(f"""
            CREATE TABLE STV202310168__DWH.s_dialog_info
            (
                hk_message_id BIGINT NOT NULL CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV202310168__DWH.h_dialogs (hk_message_id),
                message varchar(1000),
                message_from int,
                message_to int,
                load_dt datetime,
                load_src varchar(20)
            )
            ORDER BY load_dt
            SEGMENTED BY hk_message_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)

        # Create s_group_name
        cur.execute(f"""
            CREATE TABLE STV202310168__DWH.s_group_name
            	(
                hk_group_id BIGINT NOT NULL CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202310168__DWH.h_groups (hk_group_id),
                group_name varchar(100),
                load_dt datetime,
                load_src varchar(20)
            	)
            ORDER BY load_dt
            SEGMENTED BY hk_group_id ALL nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)

        # Create s_group_private_status
        cur.execute(f"""
            create table STV202310168__DWH.s_group_private_status
            (
            hk_group_id bigint not null CONSTRAINT fk_group_private_status_h_groups REFERENCES STV202310168__DWH.h_groups (hk_group_id),
            is_private boolean,
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_group_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)

        # Create s_user_chatinfo
        cur.execute(f"""
            create table STV202310168__DWH.s_user_chatinfo
            (
            hk_user_id bigint not null CONSTRAINT fk_s_admins_h_users REFERENCES STV202310168__DWH.h_users (hk_user_id),
            chat_name varchar(200),
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_user_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)


        # Create s_user_socdem
        cur.execute(f"""
            create table STV202310168__DWH.s_user_socdem
            (
            hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202310168__DWH.h_users (hk_user_id),
            chat_name varchar(200),
            country  varchar(200),
            age Integer,
            load_dt datetime,
            load_src varchar(20)
            )
            order by load_dt
            SEGMENTED BY hk_user_id all nodes
            PARTITION BY load_dt::date
            GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
        """)


        # Create 
        # project6 satellites 
        cur.execute(f"""
            CREATE TABLE STV202310168__DWH.s_auth_history
            (
                hk_l_user_group_activity int NOT NULL,
                user_id_from int,
                event varchar(20),
                event_dt timestamp,
                load_dt timestamp,
                load_src varchar(20)
            )
            ORDER BY
            load_dt
                        SEGMENTED BY hk_l_user_group_activity ALL nodes
                        PARTITION BY load_dt::date
            GROUP BY
            calendar_hierarchy_day(load_dt::date,
            3,
            2);
        """)

# Задаю функцию для заполнения таблиц слоя dds данными
def load_from_stg_to_dds_tables():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        
        #copy to hubs

        #h_users
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
            select
                   hash(id) as  hk_user_id,
                   id as user_id,
                   registration_dt,
                   now() as load_dt,
                   's3' as load_src
                   from STV202310168__STAGING.users
            where hash(id) not in (select hk_user_id from STV202310168__DWH.h_users); 
        """)

        #h_dialogs
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
            select
                   hash(message_id) as  hk_message_id,
                   message_id as message_id,
                   message_ts,
                   now() as load_dt,
                   's3' as load_src
                   from STV202310168__STAGING.dialogs
            where hash(message_id) not in (select hk_message_id from STV202310168__DWH.h_dialogs); 
        """)

        #h_groups
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
            select
                   hash(id) as  hk_group_id,
                   id as group_id,
                   registration_dt,
                   now() as load_dt,
                   's3' as load_src
                   from STV202310168__STAGING.groups
            where hash(group_id) not in (select hk_group_id from STV202310168__DWH.h_groups); 
        """)


        #h_group_log project6
        cur.execute(f"""
            INSERT
	            INTO
	            STV202310168__DWH.h_group_log (
	            hk_group_id,
                group_id,
                event_dt,
	            load_dt,
	            load_src)
                        SELECT
                hash(group_id) AS hk_group_id,
                group_id AS group_id,
                "datetime" AS event_dt,
	            now() AS load_dt,
            	's3' AS load_src
            FROM
            	STV202310168__STAGING.group_log 
            WHERE
            	hash(group_id) NOT IN (
            	SELECT
	            	hk_group_id
	            FROM
	            	STV202310168__DWH.h_group_log); 
        """)


        #copy to links

        #l_admins
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
            select
            hash(hg.hk_group_id,hu.hk_user_id),
            hg.hk_group_id,
            hu.hk_user_id,
            now() as load_dt,
            's3' as load_src
            from STV202310168__STAGING.groups as g
            left join STV202310168__DWH.h_users as hu on g.admin_id = hu.user_id
            left join STV202310168__DWH.h_groups as hg on g.id = hg.group_id
            where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV202310168__DWH.l_admins); 
        """)


        #l_user_message
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.l_user_message(
                hk_l_user_message,
                hk_user_id,
                hk_message_id,
                load_dt,
                load_src
            )
            SELECT
                hash(hk_message_id, hk_user_id),
                hk_message_id,
                hk_user_id,
                now() as load_dt,
                's3' as load_src
            FROM 
                STV202310168__STAGING.dialogs AS d
                LEFT JOIN STV202310168__DWH.h_users AS hu ON d.message_from = hu.user_id
                LEFT JOIN STV202310168__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
            WHERE hash(hk_message_id, hk_user_id) NOT IN (SELECT hk_l_user_message FROM STV202310168__DWH.l_user_message);
        """)

        #l_groups_dialogs
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.l_groups_dialogs(
                hk_l_groups_dialogs,
                hk_message_id,
                hk_group_id,
                load_dt,
                load_src
            )
            SELECT 
                hash(hd.hk_message_id, hu.hk_user_id),
                hd.hk_message_id,
                hu.hk_user_id,
               now() as load_dt,
                's3' as load_src
            FROM 
                STV202310168__STAGING.dialogs AS d
                LEFT JOIN STV202310168__DWH.h_users AS hu  ON d.message_from = hu.user_id
                LEFT JOIN STV202310168__DWH.h_dialogs AS hd  ON d.message_id = hd.message_id  
            WHERE hash(hd.hk_message_id, hu.hk_user_id) NOT IN (SELECT hk_l_groups_dialogs FROM STV202310168__DWH.l_groups_dialogs)
            AND hu.hk_user_id is not null;
        """)
        
        #l_user_group_activity  project6
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.l_user_group_activity(
            hk_l_user_group_activity, 
            hk_user_id,
            hk_group_id,
            load_dt,
            load_src)
            select distinct
                   hash(hu.hk_user_id,
                         hg.hk_group_id),
                         hu.hk_user_id,
                         hg.hk_group_id,
                         now() as load_dt,
                         's3' as load_src
            from STV202310168__STAGING.group_log as gl
            left join STV202310168__DWH.h_users as hu on gl.user_id = hu.user_id
            left join STV202310168__DWH.h_groups as hg on gl.group_id = hg.group_id;
        """)
        
        
        #salellites

        #s_admins
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
            select la.hk_l_admin_id,
            True as is_admin,
            hg.registration_dt,
            now() as load_dt,
            's3' as load_src
            from STV202310168__DWH.l_admins as la
            left join STV202310168__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id; 
        """)

        #s_user_chatinfo
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
            select 
            hu.hk_user_id,
            u.chat_name,
            now() as load_dt,
            's3' as load_src
            from STV202310168__STAGING.users as u
            LEFT join STV202310168__DWH.h_users as hu on u.id = hu.user_id; 
        """)

        #s_user_socdem
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_user_socdem(
            hk_user_id,
            chat_name,
            country,
            age,
            load_dt,
            load_src)
            select 
            hu.hk_user_id,
            u.chat_name,
            u.country,
            u.age,
            now() as load_dt,
            's3' as load_src
            from STV202310168__STAGING.users as u
            LEFT join STV202310168__DWH.h_users as hu on u.id = hu.user_id; 
        """)

        #s_group_private_status
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
            select 
            hg.hk_group_id,
            g.is_private,
            now() as load_dt,
            's3' as load_src
            FROM STV202310168__STAGING.groups AS g
            JOIN STV202310168__DWH.h_groups AS hg ON g.id = hg.group_id;
        """)

        #s_group_name
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
            SELECT
                hg.hk_group_id,
                g.group_name,
                NOW() AS load_dt,
                's3' AS load_src
            FROM STV202310168__STAGING.groups g
            JOIN STV202310168__DWH.h_groups hg ON g.id = hg.group_id;
        """)

        #s_dialog_info
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
            SELECT
                hd.hk_message_id,
                d.message,
                d.message_from,
                d.message_to,
                NOW() as load_dt,
                's3' as load_src
            FROM STV202310168__STAGING.dialogs d
            JOIN STV202310168__DWH.h_dialogs hd ON d.message_id = hd.message_id;
        """)

        #s_auth_history project6
        cur.execute(f"""
            INSERT INTO STV202310168__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
            select 
                hash(hu.user_id) AS hk_l_user_group_activity,
	            gl.user_id_from AS user_id_from,
	            gl.event AS event,
	            gl.datetime AS event_dt,
	            now() AS load_dt,
	            's3' AS load_src
            from STV202310168__STAGING.group_log as gl
            left join STV202310168__DWH.h_groups as hg on gl.group_id = hg.group_id
            left join STV202310168__DWH.h_users as hu on gl.user_id = hu.user_id
            left join STV202310168__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
            WHERE
            	hk_l_user_group_activity NOT IN (
            	SELECT
            		hk_l_user_group_activity
            	FROM
	            	STV202310168__DWH.s_auth_history);
        """)

with DAG('sprint6_vertica_project_dds_dag', 
         default_args=default_args, 
         schedule_interval='@daily',  
         catchup=False,
         tags=['sprint6', '2_dag', 'dds'],
         is_paused_upon_creation=True,
         start_date=pendulum.datetime(2023, 1, 1, tz="UTC")
        ) as dag:
     
    # Вызываю функцию, которая создаёт таблицы слоя dds с помощью pythonoperator
    create_dwh_tables = PythonOperator(
        task_id='create_dwh_tables',
        python_callable=create_dwh_tables
    )

    # Вызываю функцию, которая заполняет данными таблицы слоя dds с помощью pythonoperator
    load_from_stg_to_dds_tables = PythonOperator(
        task_id='load_from_stg_to_dds_tables',
        python_callable=load_from_stg_to_dds_tables
    )

    create_dwh_tables >> load_from_stg_to_dds_tables

