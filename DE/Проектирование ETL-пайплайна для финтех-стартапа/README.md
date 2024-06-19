# Проектирование ETL-пайплайна для финтех-стартапа

#### Описание проекта:
Спроектирован ETL-пайплайн для анализа и визуализации бизнес-метрик на основе информации о курсах валют и 
проведённых транзакциях.
* Спроектирован ETL-пайплайн для финтех-стартапа, реализована обработка данных, построена фитрина данных.
* Входными данными является информация о курсах валют за определённые периоды и информация о провёденных 
  в эти периоды транзакциях.
* Построена витрина, которая отображает аналитические метрики и dashboard.

#### Задачи проекта:
1. С помощью SparkStructuredStreaming прочитать данные из Kafka и записать их в СУБД PostgreSQL
2. С помощью SQL и AirFlow произвести предобработку данных и наполнить слои хранилища в СУБД Vertica
3. С помощью Metabase создать dashboard для отображения бизнес-метрик

#### Инструменты: 
* `SparkStructuredStreaming`, `AirFlow`, `Metabase`, `Docker`, `Kafka`, `SQL`, `PostgreSQL`, `python`, `Vertica`

#### Ключеные слова:
* Stream Processing; Apache Spark Structured Streaming; Apache Kafka; Vertica; PostgreSQL; Metabase;

#### Навыки:
- Потоковая обработка данных;
- Создание AirFlow DAG's;
- Построение пайплайна;
- Создание dashboard.

#### Сфера деятельности:
* fintech

#### Файловая структура:
Описание структуры проекта:

```
├───Проектирование ETL-пайплайна для финтех-стартапа    - Директория с финальным проектом
│   ├───dags                                            - Тут 2 дага (pg_to_vertica_dag.py и stg_to_cdm_dag.py)
│   │   └───SQL                                         - SQL-скрипты, которые используются в дагах (SELECT, DELETE, COPY)
│   ├───img                                             - Изображения
│   ├───py                                              - SparkJob (final_sprint_spark_job.py) 
│   └───sql
│       ├───Postgres                                    - DDL таблиц в Postgres
│       └───Vertica                                     - DDL таблиц в Vertica
```


1. Создан топик kafka (topik='a_merkulov_tsi') для приёма сообщений с данными о курсах валют и транзакциях.
2. В СУБД PostgreSQL созданы 2 таблицы:
```
"public.a_merkulov_currencies" - Курсы валют
"public.a_merkulov_transactions" - Транзакции
```
В DDL таблиц реализованы ограничения предотврещающие появление в таблицах полных дубликатов.
3. В СУБД Vertica созданы 3 таблицы:
Слой staging:
```
"STV202310168__STAGING.currencies" - Курсы валют
"STV202310168__STAGING.transactions" - Транзакции
```
Слой common data marts:
```
STV202310168.global_metrics
```
В DDL таблиц были реализованы ограничения предотвращающие появления в таблицах
дубликатов и аномальных значений.

4. В BI Metabase реализован dashboard Global Metrics (img/Dashboard.jpg) c визуализацией
необходимых для бизнеса метрик.
Время обновления - 1 раз в 60 минут.


Описание ETL:

1. Данные поступают из kafka (topik='a_merkulov_tsi'), обрабатываются с помощью SparkStructuredStreaming 
(final_sprint_spark_job.py) и пишутся в 2 таблицы в СУБД PostgreSQL. 
Таблицы: 
"public.a_merkulov_currencies" - Курсы валют
"public.a_merkulov_transactions" - Транзакции
В процессе вставки данных из Kafka в Vertica производится обработка дубликатов по набору полей:
```
.dropDuplicates(['object_id', 'object_type', 'sent_dttm'])
```
2. С помощью AirFlow DAG (pg_to_vertica_dag.py) данные копируются из таблиц PostgreSQL в соответствующие таблицы СУБД Vertica:
Из PostgreSQL "public.a_merkulov_currencies" в Vertica "STV202310168__STAGING.currencies"
Из PostgreSQL "public.a_merkulov_transactions" в Vertica "STV202310168__STAGING.transactions"
В процессе копирования данные проходят предобработку - очистку от дубликатов и аномальных значений.
Предобработка данных реализована в SQL скриптах.

3. С помощью AirFlow DAG (stg_to_cdm_dag.py) происходит расчёт необходимых для бизнеса метрик и заполнение витрины
STV202310168.global_metrics в СУБД Vertica.

4. На основе данных из витрины STV202310168.global_metrics строится Dashboard Global Metrics в Metabase.