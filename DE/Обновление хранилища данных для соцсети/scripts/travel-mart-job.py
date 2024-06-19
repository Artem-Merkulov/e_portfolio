#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Джоба для расчёта витрины путешествий пользователя


# In[4]:


# Импорты

import sys
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, to_date, to_timestamp, date_format, unix_timestamp, from_unixtime, expr, split, date_add
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType, LongType
from pyspark.sql import SQLContext 
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


# In[20]:


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
    
    # Читаю geo.csv с hdfs и записываю результат в переменную
    geo_csv = read_geo_csv(geo_csv_paths, spark)
    
    # Читаю events_geo с hdfs
    events_geo = read_events_geo(events_geo_paths, spark)
    
    # Ищем в каком городе было совершено событие. Окно разбиваю по message_id.
    city_dist = get_city_dist(events_geo, geo_csv) 
    
    # Тут цепочка всех событий для каждого пользователя.
    sequence_of_events = get_sequence_of_events(city_dist, city_dist)
    
    # Строю витрину
    travel_mart = get_travel_mart(sequence_of_events)
    
    # Выполняю функцию для записи витрины на hdfs
    writer = partition_writer(travel_mart)

    # Записываю на хдфс
    writer.save(f'{base_output_path}/travel_mart/date={date}/')


# In[10]:


def partition_writer(events):
    return events \
        .write \
        .mode('overwrite') \
        .partitionBy('user_id') \
        .format('parquet')


# In[11]:


# Задаю функцию, которая прочитает geo.csv с hdfs
# Меняю запятую на точку в полях с координатами и привожу их тип к типу float.
def read_geo_csv(geo_csv_paths, spark):
    geo = spark.read.option("delimiter", ";").option("header", True).csv(f'{geo_csv_paths}')\
                                             .persist()                                                 
    return geo


# In[21]:


# Задаю функцию, которая прочитает обновлённый файл events_geo, с координатами отправленных сообщений, с hdfs 
# Использую тут метод .sample(0.5)
# Вместо message_id, создаю поле event_id с помощью функции monotonically_increasing_id()
def read_events_geo(events_geo_paths, spark):
    geo = spark.read.parquet(f'{events_geo_paths}').sample(0.5)\
                         .drop('city','id')\
                         .withColumn('event_id', F.monotonically_increasing_id())\
                         .select(F.col("event_id").alias("event_id"),\
                          F.col("event.message_from").alias("message_from"), \
                          F.col("event.message_ts").alias("message_ts"), \
                          F.col("lat").alias("msg_lat"),\
                          F.col("lon").alias("msg_lon"))\
                          .persist()
    return geo


# In[13]:


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
            F.pow(F.sin((F.radians(F.col("msg_lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
            F.cos(F.radians(F.col("msg_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
            F.pow(F.sin((F.radians(F.col("msg_lon")) - F.radians(F.col("city_lon"))) / 2), 2)
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


# In[14]:


# Задаю функцию, которая подготовит данные для расчёта витрины и выстроит последовательность действий для каждого пользователя
# Убираю лишние поля и строки
# Нахожу непрерывную последовательность событий для каждого пользователя
def get_sequence_of_events(city_coors, city_dist):

    sequence = city_dist.where("message_from is not null")\
                                       .where("message_ts is not null")\
                                       .select(F.col("message_from").cast("Int").alias("user_id"),\
                                       F.col("event_id").cast("Int").alias("event_id"),\
                                       F.col("city_id").cast("Int").alias("city_id"),\
                                       F.col("city_name").alias("city_name_au"),\
                                       F.col("utc_diff").alias("utc_diff"),\
                                       F.col("message_ts").cast("Timestamp").alias("message_ts"))\
                                       .withColumn("city_name", F.concat(F.lit("Australia/"), F.col('city_name_au')))\
                                       .drop('city_name_au')\
                                       .withColumn('timestamp_hours', unix_timestamp(col('utc_diff'), 'HH:mm'))\
                                       .withColumn("local_time", from_unixtime(unix_timestamp("message_ts") + col("timestamp_hours")).cast("Timestamp"))\
                                       .drop('timestamp_hours', 'utc_diff')\
                                       .withColumn("message_date", date_format('message_ts', "yyyy-MM-dd").cast("Date"))\
                                       .orderBy(F.asc("user_id"), F.asc("message_ts"))\
                                       .persist()
    return sequence  


# In[15]:


# days_cnt - количество последовательных дней, проведённых в одном городе
# last_date - дата последнего дня в последней группе городов
# Отсекаем группы в которых пользователь находился менее 27
# Среди оставшихся городов находим последний посещённый пользователем
# На тестовом df отработало успешно (поставил .filter(F.col('days_cnt') > 5)) -> 6      |1      |vlg
# Тут окна для определения rn и grp поменял. Было просто по user_id. Неправильно grp считалось.

def get_home_city(sequence_of_events: DataFrame) -> DataFrame:

    home_city = sequence_of_events.select(F.col("user_id").alias("user_id"),
                                          F.col("city_id").alias("city_id"),
                                          F.col("city_name").alias("home_city"),
                                          F.col("message_date").alias("message_date"))\
                                   .distinct()\
                                   .withColumn("rn", F.row_number().over(Window().partitionBy('user_id', 'city_id', 'home_city').orderBy(F.col('message_date').asc())))\
                                   .withColumn("grp", F.expr("date_sub(message_date, rn)"))\
                                   .orderBy(F.col('message_date').asc())\
                                   .groupBy('user_id', 'city_id', 'home_city', 'grp').agg(F.max(col("message_date")).alias('last_date'), F.count(col("*")).alias('city_cnt'))\
                                       .filter(F.col('city_cnt') > 27)\
                                       .withColumn("last_date", F.row_number()\
                                                 .over(Window().partitionBy('user_id').orderBy(F.col('last_date').desc())))\
                                       .filter(F.col('last_date') == 1)\
                                  .select('user_id', 'city_id', 'home_city')
      
    return home_city


# In[16]:


# Определяю последний город, в котором пользователь совершил своё последнее событие.

# Ради интереса эту функцию я написал тоже вместе с жпт, но возможно и сам написал бы.
# Жпт написала логику, но криво - пришлось самому править.

# Разберу что в ней происходит:

# Создаю окна по идентификатору пользователя и сортирую строки в этих окнах по времени совершения события, по убыванию.
# Далее применяю оконную функцию row_number и нумерую строки в окнах (как классно, что я работаю с оконными функциями - я начинаю
# всё лучше понимать, как они работают). 
# Оставляю только строки с номером ("rank") == 1, то есть с временем последнего отправленного сообщения.
# Соответственно в строках с временем последнего отправленного сообщения будет так же определён город, 
# в котором пользователь совершил своё последнее действие.

def find_act_city(sequence_of_events: DataFrame) -> DataFrame:
    
    act_city = sequence_of_events.withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(col("message_ts").desc())))\
                                         .filter(col("rank") == 1)\
                                         .select(F.col("user_id").alias("user_id"),\
                                                 F.col("city_id").alias("city_id"),\
                                                 F.col("city_name").alias("act_city"),\
                                                 F.col("local_time").alias("local_time"))
    return act_city


# In[17]:


def get_travel_count(sequence_of_events: DataFrame) -> DataFrame:

    travel_count = sequence_of_events.select('user_id', 'city_id', 'city_name', 'message_date')\
                                       .distinct()\
                                       .withColumn("rn", F.row_number().over(Window().partitionBy('user_id', 'city_id', 'city_name').orderBy(F.col('message_date').asc())))\
                                   .withColumn("grp", F.expr("date_sub(message_date, rn)"))\
                                   .orderBy(F.col('message_date').asc())\
                                   .groupBy("user_id") \
                                       .agg(F.countDistinct("grp").alias("travel_count"),\
                                            F.collect_list("city_id").alias("city_id_arr"),\
                                            F.collect_list("city_name").alias("travel_array"))                        
    return travel_count


# In[ ]:


def get_travel_mart(sequence_of_events):

    # Нахожу метрики и записываю их в переменные
    home_city = get_home_city(sequence_of_events)
    act_city = find_act_city(sequence_of_events)
    travel_count = get_travel_count(sequence_of_events)

    # Соединяю результаты выполнения функций и строю витрину
    travel_mart = act_city.join(home_city, "user_id", "left")\
                          .join(travel_count, "user_id", "left")\
                          .select('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
    return travel_mart


# In[19]:


if __name__ == "__main__":
    main()


# In[ ]:


# Команда запуска джобы для витрины путешествий
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/travel-mart-job.py /user/ackaipain/data/tmp/geo_csv_with_timezones.csv /user/master/data/geo/events /user/ackaipain/data/tmp/

