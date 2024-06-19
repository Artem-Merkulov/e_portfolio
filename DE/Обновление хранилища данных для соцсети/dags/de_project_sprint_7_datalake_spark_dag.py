#!/usr/bin/env python
# coding: utf-8

# In[2]:


import datetime
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os


# In[3]:


os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


# In[5]:


default_args = {
                                'owner': 'airflow',
                                'depends_on_past': False
                                }


# In[ ]:


dag_spark = DAG(
                        dag_id = "sprint7_pySpark_DAG_with_spark",
                        default_args=default_args,
                        tags=['sprint7', 'bash_dag'],
                        schedule_interval='@daily',
                        is_paused_upon_creation=True,
                        start_date = pendulum.datetime(2024, 4, 5, tz="UTC")
                        )


# In[ ]:


# объявляем задачу с помощью SparkSubmitOperator
travel_mart = SparkSubmitOperator(
                        task_id='travel_mart_task',
                        dag=dag_spark,
                        application ='/lessons/travel-mart-job.py' ,
                        conn_id = 'yarn_spark',
                        application_args = ['/user/ackaipain/data/tmp/geo_csv_with_timezones.csv', '/user/master/data/geo/events', '/user/ackaipain/data/tmp/']
                        )



# In[ ]:


# объявляем задачу с помощью SparkSubmitOperator
friends_recommendation = SparkSubmitOperator(
                        task_id='friends-recommendation_mart_task',
                        dag=dag_spark,
                        application ='/lessons/friends_recommendation_mart_job.py' ,
                        conn_id = 'yarn_spark',
                        application_args = ['/user/ackaipain/data/tmp/geo_csv_with_timezones.csv', '/user/master/data/geo/events', '/user/ackaipain/data/tmp/']
                        )


# In[ ]:


travel_mart >> friends_recommendation