#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator



# In[ ]:


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


# In[ ]:


default_args = {
                                'owner': 'airflow',
                                'depends_on_past': False
                                }


# In[1]:


bash_travel_mart = '/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/travel-mart-job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/'

bash_events_by_zones = '/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/events_by_zones_mart_job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/'

bash_friends_recommendation = '/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/friends_recommendation_mart_job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/'


# In[ ]:


dag_spark = DAG(
                        dag_id = "sprint7_pySpark_DAG_with_bash",
                        default_args=default_args,
                        tags=['sprint7', 'bash_dag'],
                        schedule_interval='@daily',
                        is_paused_upon_creation=True,
                        start_date = pendulum.datetime(2024, 4, 5, tz="UTC")
                        ) 


# In[ ]:


# travel mart job
travel_mart = BashOperator(
    task_id='travel_mart_task',
    bash_command=bash_travel_mart,
        retries=1,
        dag=dag_spark
)

# friends_recommendation 
friends_recommendation = BashOperator(
    task_id='friends_recommendation_task',
    bash_command=bash_friends_recommendation,
        retries=1,
        dag=dag_spark
)


travel_mart >> friends_recommendation

