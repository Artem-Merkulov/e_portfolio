#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Витрина для рекомендации друзей


# In[2]:


# Импорты

import sys
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


# In[ ]:


# Основная функция джобы
def main():

    # Путь до geo.csv на hdfs
    geo_csv_paths = sys.argv[1]
    # Путь до geo_events на hdfs, то есть обновлённая events с координатами
    events_geo_paths = sys.argv[2]
    # Путь до travel-mart на hdfs
    base_output_path = sys.argv[3]
    
    date = datetime.date.today().strftime('%y-%m-%d')   
    
    # Подключаюсь к БД  
    spark = SparkSession.builder.master("yarn")\
    .config("spark.driver.memory", "4g")\
    .config("spark.submit.deploymode", "cluster")\
    .config("spark.dynamicAllocation.enabled", "true")\
    .config("spark.dynamicAllocation.initialExecutors", "1")\
    .config("spark.dynamicAllocation.minExecutors", "0")\
    .config("spark.dynamicAllocation.maxExecutors", "6")\
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s")\
    .appName("project7DatalakeApp").getOrCreate()
    
    # Запускаю функции и записываю результат их выполнения в переменные

    # Читаю таблицу с городами и их координатами и временными зонами
    geo_csv = read_geo_csv(spark, geo_csv_paths)

    # Читаю фрейм с событиями и информацией о них
    events_geo = read_events_geo(spark, events_geo_paths)

    # Нахожу пользователей, которые подписаны на одинаковые каналы
    possible_friends = get_possible_friends(events_geo)

    # Нахожу пользователей, которые подписаны на одинаковые каналы и ранее не общались
    unsubscribers = get_unsubscribers(events_geo, possible_friends)

    # Нахожу таймстамп и координаты последнего события для каждого пользователя
    last_events = find_last_events(events_geo)
    
    # Добавляю координаты для user_id2
    user_list_all = add_coors(unsubscribers, last_events)

    # Нахожу пары пользователей, расстояние между которыми составляет менее аднаво километра
    users_dist = find_users_dist(user_list_all)

    # Строю итоговую витрину friends_recommendation_mart
    friends_recommendation_mart = create_friends_recommendation_mart(users_dist, geo_csv)
    
    # Выполняю функцию для записи витрины на hdfs
    writer = partition_writer(friends_recommendation_mart)

    # Записываю на хдфс
    writer.save(f'{base_output_path}/friends_recommendation_mart/date={date}/')


# In[ ]:


def partition_writer(events):
    return events \
        .write \
        .mode('overwrite') \
        .format('parquet')


# In[ ]:


# Задаю функцию, которая прочитает geo.csv с hdfs
# Меняю запятую на точку в полях с координатами и привожу их тип к типу float.
def read_geo_csv(spark, geo_csv_paths):
    geo = spark.read.option("delimiter", ";").option("header", True).csv(f'{geo_csv_paths}')\
                                                                                           
    return geo


# In[ ]:


# Задаю функцию, которая прочитает обновлённый файл events_geo, с координатами отправленных сообщений, с hdfs 
# Использую тут метод .sample(0.05)
# Вместо message_id, создаю поле event_id с помощью функции monotonically_increasing_id()
def read_events_geo(spark, events_geo_paths):
    geo = spark.read.parquet(f'{events_geo_paths}').sample(0.05)\
                    .drop('city','id')\
                         .withColumn('event_id', F.monotonically_increasing_id())
    return geo


# In[ ]:


# Подписаны на одинаковые каналы
def get_possible_friends(events_geo: DataFrame) -> DataFrame:
    
    # Оставляю только подписки с нужными полями
    subscriptions = events_geo\
                               .filter(F.col('event_type') == 'subscription')\
                               .where('event_type is not null')\
                               .where('event.user is not null')\
                               .select(F.col('event.user').alias('user_id'),\
                                       F.col('event.subscription_channel').alias('channel'))\
                               .distinct()

    # Получаем пары пользователей, кандидатов в друзья.
    possible_friends = subscriptions\
                                     .join(subscriptions
                                          .withColumnRenamed('user_id', 'user_id2'), ['channel'], 'inner') \
                                     .filter('user_id != user_id2')\
                                     .select(F.col('user_id').alias('user_id1'),\
                                             F.col('user_id2').alias('user_id2'))\
                                     .distinct()
    
    return possible_friends


# In[ ]:


# Подписаны на одинаковые каналы и ранее не общались

def get_unsubscribers(events_geo: DataFrame, possible_friends: DataFrame) -> DataFrame:
    
    # Оставляю только сообщения с нужными полями
    messages = events_geo\
                               .filter(F.col('event_type') == 'message')\
                               .where('event_type is not null')\
                               .where('event.message_from is not null')\
                               .where('event.message_to is not null')\
                               .select(F.col('event.message_from').alias('user_id1'),\
                                       F.col('event.message_to').alias('user_id2'))\
                               .distinct()

    # Соединяю и наоборот
    users_pairs = messages.union(messages.select('user_id2', 'user_id1')).distinct()

    # Проверим, кто из пользователей с одинаковыми подписками на каналы еще не общался
    unsubscribers = possible_friends.join(users_pairs, ['user_id1', 'user_id2'], 'left_anti')
    
    return unsubscribers


# In[ ]:


# Нахожу координаты последнего события 
def find_last_events(events_geo: DataFrame) -> DataFrame:

    last_events = events_geo\
                             .where('event.message_from is not null')\
                             .where('event.message_ts is not null')\
                             .select(F.col('event.message_id').alias('event_id'),\
                                     F.col('event.message_from').alias('user_id'),\
                                     F.col('event.message_ts').alias('message_ts'),\
                                     F.col('lat').alias('evnt_lat'),\
                                     F.col('lon').alias('evnt_lon'))\
                             .withColumn("rn", F.row_number().over(Window().partitionBy('user_id').orderBy(F.col('message_ts').desc()))) \
                             .filter(F.col('rn') == 1) \
                             .drop(F.col('rn'))
    
    return last_events


# In[3]:


def add_coors(unsubscribers, last_events):
    
    # Добавляю координаты для user_id1
    user_list = unsubscribers.join(last_events, unsubscribers.user_id1 == last_events.user_id,'inner').drop(last_events['user_id'])  \
            .withColumnRenamed('evnt_lat','lat_user1') \
            .withColumnRenamed('evnt_lon','lon_user1')

    # Добавляю координаты для user_id2
    user_list_all = user_list\
        .alias('ul')\
        .join(last_events.alias('le'), col('ul.user_id2') == col('le.user_id'), 'inner')\
        .drop('le.user_id', 'user_id', 'le.message_ts')\
        .withColumnRenamed('evnt_lat', 'lat_user2')\
        .withColumnRenamed('evnt_lon', 'lon_user2')\
        .select(F.col('user_id1'),\
                F.col('user_id2'),\
                F.col('lat_user1').cast('Float'),\
                F.col('lon_user1').cast('Float'),\
                F.col('lat_user2').cast('Float'),\
                F.col('lon_user2').cast('Float'),\
                F.col('ul.event_id'),\
                F.col('ul.message_ts'))
    
    return user_list_all


# In[ ]:


def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371.0 # Радиус Земли в километрах

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance_km = R * c

    return distance_km


# In[ ]:


# Нахожу пары пользователей, расстояние между которыми составляет менее аднаво километра
def find_users_dist(user_list_all: DataFrame) -> DataFrame:

    # Применяю UDF к DataFrame с координатами точек
    users_dist = user_list_all.withColumn("distance_km", calculate_distance(user_list_all['lat_user1'],\
                                                                            user_list_all['lon_user1'],\
                                                                            user_list_all['lat_user2'],\
                                                                            user_list_all['lon_user2']))\
                              .select(F.col('user_id1'),\
                                      F.col('user_id2'),\
                                      F.col('lat_user1').alias('evnt_lat'),\
                                      F.col('lon_user1').alias('evnt_lon'),\
                                      F.col('distance_km').cast('Float'),\
                                      F.col('message_ts').cast('TimeStamp'),\
                                      F.col('event_id').cast('Int'))\
                              .filter(F.col('distance_km')<=1)
    
    return users_dist


# In[ ]:


# Нахожу расстояние между парой пользователей и близжайшей зоной (городом)
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


# Строю итоговую витрину friends_recommendation_mart

def create_friends_recommendation_mart(users_dist: DataFrame, geo_csv: DataFrame) -> DataFrame:

    # Добавляю информацию о зонах (городах) Австралии и добавляю необходимые поля
    # Такие как processed_dttm, local_time
    # Сортирую DataFrame так чтобы было красиво

    friends_recommendation_mart = get_city_dist(users_dist, geo_csv)\
                             .withColumn('processed_dttm', current_date())\
                             .select(F.col('user_id1').cast('Int').alias('user_left'),\
                                     F.col('user_id2').cast('Int').alias('user_right'),\
                                     F.col('processed_dttm').cast('Date'),\
                                     F.col('city_id').cast('Int').alias('zone_id'),\
                                     F.col(('message_ts')),\
                                     F.col('utc_diff'))\
                                     .withColumn('timestamp_hours', unix_timestamp(col('utc_diff'), 'HH:mm'))\
                                     .withColumn("local_time", from_unixtime(unix_timestamp("message_ts") + col("timestamp_hours")).cast("Timestamp"))\
                                     .drop('timestamp_hours', 'utc_diff', 'message_ts')\
                             .orderBy(F.asc("user_left"), F.asc("user_right"), F.asc("zone_id"), F.asc("local_time"))
    
    return friends_recommendation_mart


# In[ ]:


if __name__ == "__main__":
    main()


# In[ ]:


# Команда запуска джобы
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/friends_recommendation_mart_job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/

