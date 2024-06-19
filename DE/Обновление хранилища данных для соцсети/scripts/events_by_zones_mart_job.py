#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Импорты

import sys
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


# In[ ]:


# Главная функция

def main():
    
    # Путь до geo.csv на hdfs
    geo_csv_paths = sys.argv[1]
    # Путь до geo_events на hdfs, то есть обновлённая events с координатами
    events_geo_paths = sys.argv[2]
    # Путь до travel-mart на hdfs
    base_output_path = sys.argv[3]
    
    date = datetime.date.today().strftime('%y-%m-%d')
    
    spark = SparkSession \
    .builder.master("yarn") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.cores", "4") \
    .config("spark.ui.port", "4051") \
    .appName(f"TravelMartJob-{date}") \
    .getOrCreate()
    
    # Запускаю функции и записываю результат их выполнения в переменные
    
    # Читаю данные на которых будут основаны вычисления и построена витрина
    geo_csv = read_geo_csv(geo_csv_paths, spark)
    events_geo = read_events_zones(events_geo_paths, spark)
    
    # Вычисляю город, в котором было совершено событие, сопоставляя координаты совершения события и городов
    city_dist = get_city_dist(events_geo, geo_csv)
    
    # Считаю метрики для реакций
    reactions = get_reactions(city_dist)
    
    # Считаю метрики для подписок
    subscriptions = get_subscriptions(city_dist)
    
    # Считаю метрики для сообщений
    messages = get_messages(city_dist)
    
    # Считаю метрики для регистраций
    registrations = get_registrations(city_dist)
    
    # вычисляю уникальные даты
    dates = get_dates(city_dist)
    
    # Строю витрину events_by_zones
    events_by_zones = get_events_by_zones(dates, registrations, messages, reactions, subscriptions)
    
    # Выполняю функцию для записи витрины на hdfs
    writer = partition_writer(events_by_zones)

    # Записываю на хдфс
    writer.save(f'{base_output_path}/events_by_zones_mart/date={date}/')


# In[ ]:


def partition_writer(events):
    return events \
        .write \
        .mode('overwrite') \
        .partitionBy('month') \
        .format('parquet')


# In[ ]:


# Задаю функцию, которая прочитает geo.csv с hdfs
# Меняю запятую на точку в полях с координатами и привожу их тип к типу float.

def read_geo_csv(geo_csv_paths, spark):
    geo = spark.read.option("delimiter", ";").option("header", True).csv(f'{geo_csv_paths}')\
                                             .persist()                                                 
    return geo

# In[ ]:


# Оставляю от датафрейма с событиями только нужные для расчёта витрины поля:
# Пока изучаю данные

def read_events_zones(events_geo_paths, spark):
    zones = spark.read.parquet(f'{events_geo_paths}').sample(0.5)\
                   .withColumn('event_id', F.monotonically_increasing_id())\
                         .select(F.col("event_type").alias("event_type"),\
                                 F.col("event.user").alias("user_id"),\
                                 F.col("event_id").alias("event_id"),\
                                 F.col("date").alias("date"),\
                                 F.col("event.message_ts").alias("message_ts"), \
                                 F.col("event.message_from").alias("message_from"), \
                                 F.col("event.reaction_from").alias("reaction_from"), \
                                 F.col("event.subscription_user").alias("subscription_user"),\
                                 F.col("lat").alias("evnt_lat"),\
                                 F.col("lon").alias("evnt_lon"))\
                                 .persist()
    return zones


# In[ ]:


# Функция для расчёта расстояния между городами
# Тут мы присоединяем ВСЕ города с помощью .crossJoin(geo_city) и выбираем близжайший .filter(F.col('row_number')==1)
# Тут есть расхождение с авторским кодом:
# В авторском (если то, что мне дали - это авторский) окно разбивается по .partitionBy('event.message_from')
# Тут .partitionBy('event_id') 
# Так как в авторском берется сообщение, которое было отправлено ближе к центру какого-то города, а не первое отправленное

# Режу окно по event_id и по этому окну нахожу, из какого города было отправлено сообщение .withColumn('diff', calculate_diff)
# Позже готовую витрину сгруппирую по message_from (там будет называться user_id)
# Записываю результат на диск
def get_city_dist(events_geo, geo_city) -> DataFrame:

    EARTH_R = 6371

    calculate_diff = 2 * F.lit(EARTH_R) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.radians(F.col("evnt_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
            F.cos(F.radians(F.col("evnt_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
            F.pow(F.sin((F.radians(F.col("evnt_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
        )
    )
    
    window = Window().partitionBy('event_id').orderBy(F.col('diff').asc())
    events_city = events_geo \
        .crossJoin(geo_city) \
        .withColumn('diff', calculate_diff)\
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number') 

    return events_city


# In[ ]:


def get_city_dist_columns(events_zones: DataFrame, geo_csv: DataFrame) -> DataFrame:

    dist = get_city_dist(events_zones, geo_csv)\
                                 .select(F.col("event_type"),\
                                 F.col("user_id"),\
                                 F.col("event_id"),\
                                 F.col("date"),\
                                 F.col("message_ts"), \
                                 F.col("message_from"), \
                                 F.col("reaction_from"), \
                                 F.col("subscription_user"),\
                                 F.col("city_id"))
    return dist


# In[ ]:


# Количество реакций за месяц и за неделю

def get_reactions(city_dist: DataFrame) -> DataFrame:
    
    reactions = city_dist.filter(F.col('event_type') == 'reaction')\
                         .select(F.col("date").cast("Date"), \
                                 F.col("city_id").alias("zone_id"),\
                                 F.col("event_id").cast("Int"))\
                         .withColumn('month', trunc(col("date"),"Month").alias("month"))\
                         .withColumn("week", trunc(col("date"),"Week").alias("week"))\
                         .groupBy('month', 'week', 'zone_id').agg(F.count("event_id").alias("week_reaction"))\
                         .withColumn("month_reaction", F.sum('week_reaction')\
                                          .over(Window().partitionBy('month', 'zone_id').orderBy(F.col('month').asc())))\
                         .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    return reactions


# In[ ]:


# Количество подписок за месяц и за неделю

def get_subscriptions(city_dist: DataFrame) -> DataFrame:

    subscriptions = city_dist.filter(F.col('event_type') == 'subscription')\
                                  .select(F.col("date").cast("Date"),\
                                          F.col("city_id").alias("zone_id"),\
                                          F.col("event_id").cast("Int"))\
                                  .withColumn('month', trunc(col("date"),"Month").alias("month"))\
                                  .withColumn("week", trunc(col("date"),"Week").alias("week"))\
                                  .groupBy('month', 'week', 'zone_id').agg(F.count("event_id").alias("week_subscription"))\
                         .withColumn("month_subscription", F.sum('week_subscription')\
                                          .over(Window().partitionBy('month', 'zone_id').orderBy(F.col('month').asc())))\
                         .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    return subscriptions


# In[ ]:


# Количество сообщений за месяц и за неделю

def get_messages(city_dist: DataFrame) -> DataFrame:

    messages = city_dist.filter(F.col('event_type') == 'message')\
                    .where("message_ts is not null")\
                                  .select(F.col("message_ts").cast("Date"),\
                                          F.col("city_id").alias("zone_id"),\
                                          F.col("event_id").cast("Int"))\
                                  .withColumn('month', trunc(col("message_ts"),"Month").alias("month"))\
                                  .withColumn("week", trunc(col("message_ts"),"Week").alias("week"))\
                                  .groupBy('month', 'week', 'zone_id').agg(F.count("event_id").alias("week_message"))\
                                  .withColumn("month_message", F.sum('week_message')\
                                          .over(Window().partitionBy('month', 'zone_id').orderBy(F.col('month').asc())))\
                                  .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    return messages


# In[ ]:


# Количество регистраций за месяц и за неделю

def get_registrations(city_dist: DataFrame) -> DataFrame:

    registrations = city_dist.filter(F.col('event_type') == 'message').where("message_from is not null")\
                                  .where("message_ts is not null")\
                                  .select(F.col("message_from").cast("Int").alias("user_id"),\
                                          F.col("message_ts").cast("Date"),\
                                          F.col("city_id").alias("zone_id"),\
                                          F.col("event_id").cast("Int"))\
                                  .withColumn("rn", F.row_number()\
                                          .over(Window().partitionBy('user_id').orderBy(F.col('message_ts').asc())))\
                                          .filter(F.col('rn') == '1')\
                                  .drop("rn")\
                                  .withColumn('month', trunc(col("message_ts"),"Month").alias("month"))\
                                  .withColumn("week", trunc(col("message_ts"),"Week").alias("week"))\
                                  .groupBy('month', 'week', 'zone_id').agg(F.count("event_id").alias("week_user"))\
                                  .withColumn("month_user", F.sum('week_user')\
                                          .over(Window().partitionBy('month', 'zone_id').orderBy(F.col('month').asc())))\
                                  .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    return registrations


# In[ ]:


# Формирую признак группировки для витрины

def get_dates(city_dist: DataFrame) -> DataFrame:

    dates = city_dist\
                  .select(F.col('date').cast("Date"),\
                          F.col('city_id').cast("Int").alias("zone_id"))\
                                                      .distinct()\
                                                      .withColumn('month', trunc(col("date"),"Month").alias("month"))\
                                                      .withColumn("week", trunc(col("date"),"Week").alias("week"))\
                  .drop('date')\
                  .select('month', 'week', 'zone_id')\
                  .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    return dates


# In[ ]:


# Строю витрину events_by_zones

def get_events_by_zones(dates: DataFrame, registrations: DataFrame, messages: DataFrame, reactions: DataFrame, subscriptions: DataFrame, ) -> DataFrame:

    events_by_zones = dates.join(registrations, ['month', 'week', 'zone_id'], 'outer') \
                       .join(messages, ['month', 'week', 'zone_id'], 'outer') \
                       .join(reactions, ['month', 'week', 'zone_id'], 'outer') \
                       .join(subscriptions, ['month', 'week', 'zone_id'], 'outer') \
                       .select(dates['month'], \
                               dates['week'], \
                               dates['zone_id'], \
                               messages['week'].alias('week_message'), \
                               reactions['week'].alias('week_reaction'), \
                               subscriptions['week'].alias('week_subscription'), \
                               registrations['week_user'], \
                               messages['month'].alias('month_message'), \
                               reactions['month'].alias('month_reaction'), \
                               subscriptions['month'].alias('month_subscription'), \
                               registrations['month_user'])\
                       .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    
    return events_by_zones


# In[ ]:


# Строю витрину events_by_zones

def get_events_by_zones(dates: DataFrame, registrations: DataFrame, messages: DataFrame, reactions: DataFrame, subscriptions: DataFrame, ) -> DataFrame:

    events_by_zones = dates.join(registrations, ['month', 'week', 'zone_id'], 'outer') \
                       .join(messages, ['month', 'week', 'zone_id'], 'outer') \
                       .join(reactions, ['month', 'week', 'zone_id'], 'outer') \
                       .join(subscriptions, ['month', 'week', 'zone_id'], 'outer') \
                       .select(dates['month'], \
                               dates['week'], \
                               dates['zone_id'], \
                               messages['week_message'], \
                               reactions['week_reaction'], \
                               subscriptions['week_subscription'], \
                               registrations['week_user'], \
                               messages['month_message'], \
                               reactions['month_reaction'], \
                               subscriptions['month_subscription'], \
                               registrations['month_user'])\
                       .orderBy(F.asc("month"), F.asc("week"), F.asc("zone_id"))
    
    return events_by_zones


# In[ ]:


if __name__ == "__main__":
    main()


# In[ ]:


# Команда запуска джобы для витрины путешествий
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/events_by_zones_mart_job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/

