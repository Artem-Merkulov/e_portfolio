#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Импорты
import psycopg2
import sqlalchemy
import pandas as pd


# In[16]:


conn = psycopg2.connect("host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")


# In[17]:


# Создаю таблицу в базе Postgres
with conn.cursor() as cur:
            cur.execute(
"""CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);""")
conn.commit()


# In[22]:


# Создаю движок
engine = sqlalchemy.create_engine('postgresql://jovyan:jovyan@localhost:5432/de')


# In[23]:


# Читаю таблицу из базы и сохраняю её в переменную
subscribers_feedback = pd.read_sql_table(
    "subscribers_feedback",
    con=engine,
    index_col = 'id'
)


# In[24]:


# Вывожу таблицу на экран
display(subscribers_feedback);


# In[21]:


# Создаю таблицу в базе Postgres
with conn.cursor() as cur:
            cur.execute(
"""TRUNCATE TABLE public.subscribers_feedback;""")
conn.commit()

