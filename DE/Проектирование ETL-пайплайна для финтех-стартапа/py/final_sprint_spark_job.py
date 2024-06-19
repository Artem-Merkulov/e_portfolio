# %%
# Инициализирую Spark
import findspark
findspark.init()

# Стандартные библиотеки python
import os
import logging
from time import sleep
from datetime import datetime

# Прописываю домашние пути перед импортом SparkSession
os.environ['JAVA_HOME'] = '/usr/local/openjdk-11'
os.environ['SPARK_HOME'] = '/opt/spark'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYTHONPATH'] ='/usr/lib/python3.9'

# Модули pySpark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

# %%
# Формирую переменную с датой
date = datetime.today().strftime('%y-%m-%d')

# %%
# Задаю параметры логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__) 

# %%
# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# %%
# Функция для открытия SparkSession
def create_spark_session(app_name: str, spark_jars_packages: str) -> SparkSession:

    return SparkSession.builder.master('local').config("spark.jars.packages", spark_jars_packages).appName(app_name).getOrCreate()

# %%
# Открываем SparkSession
spark = create_spark_session(f"FinalProjectStreamingService-{date}", spark_jars_packages) 

# %%
# Прописываю опции для доступа по ssl
truststore_location = "/etc/security/ssl"
truststore_pass = "de_sprint_8"

# %%
# Опции подключения к Kafka
kafka_security_options = {
    'kafka.bootstrap.servers':'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

# %%
# Топики Kafka
kafka_topik_in = 'a_merkulov_tsi'
kafka_topik_out = 'a_merkulov_test'

# %%
# читаем из топика Kafka сообщения с акциями от ресторанов 
read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', kafka_topik_in) \
    .load()

# %%
# Определяем схему входного сообщения для json
incomming_message_schema = StructType([
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("sent_dttm", TimestampType(), True),
        StructField("payload", StringType(), True)
    ])

# %%
# Десериализую данные из входящего сообщения JSON
df_json = (read_stream_df.withColumn('key_string', F.col('key').cast(StringType())) 
                                            .withColumn('value_json', F.col('value').cast(StringType())) 
                                            .drop('key', 'value'))

# %%
# Раскладываю сообщеня из JSON по полям DataFrame
df_string = (df_json 
                            .withColumn('key', F.col('key_string')) 
                            .withColumn('value', F.from_json(F.col('value_json'), incomming_message_schema)) 
                            .drop('key_str', 'value_json'))

# %%
streaming_df = (df_string.select("value.*")).dropDuplicates(['object_id', 'object_type', 'sent_dttm'])

# %%
def handle_exception(e, df_name):
    err_message = str(e)
    if "Duplicate key value violates unique constraint!" in err_message:
        logger.error(f"{df_name}. A duplicate was detected! Skipping duplicate entry.")
    else:
        logger.error(f"An error occurred in {df_name}: {err_message}")

# %%
def upsert_to_db(df, table_name):
        df.write.format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:postgresql://rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net:6432/db1") \
            .option('driver', 'org.postgresql.Driver') \
            .option("dbtable", table_name) \
            .option("user", "student") \
            .option("password", "de_student_112022") \
            .save()

# %%
def foreach_batch_function(result_df, epoch_id):
    
    # Cохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka.
    result_df.persist()
    # Записываем df в PostgreSQL как есть.
    
    # Фильтруем данные по полю 'object_type' и записываем их в отдельные DataFrame
    currency_df = result_df.filter(F.col("object_type") == "CURRENCY")
    transaction_df = result_df.filter(F.col("object_type") == "TRANSACTION")
    
    # Преобразуем поле "payload" в формат JSON
    cur_df_payload = currency_df.withColumn("payload_json", F.from_json(F.col("payload"), "map<string,string>"))
    trn_df_payload = transaction_df.withColumn("payload_json", F.from_json(F.col("payload"), "map<string,string>"))
    
    # Обращаемся к значениям в поле "payload" по ключам
    cur_df_result = cur_df_payload.select(F.col("payload_json.date_update").cast(TimestampType()).alias("date_update"),
                                          F.col("payload_json.currency_code").cast(IntegerType()).alias("currency_code"),
                                          F.col("payload_json.currency_code_with").cast(IntegerType()).alias("currency_code_with"),
                                          F.col("payload_json.currency_with_div").cast(FloatType()).alias("currency_with_div"))

    trn_df_result = trn_df_payload.select(F.col("payload_json.operation_id").cast(StringType()).alias("operation_id"),
                                          F.col("payload_json.account_number_from").cast(IntegerType()).alias("account_number_from"),
                                          F.col("payload_json.account_number_to").cast(IntegerType()).alias("account_number_to"),
                                          F.col("payload_json.currency_code").cast(IntegerType()).alias("currency_code"),
                                          F.col("payload_json.country").cast(StringType()).alias("country"),
                                          F.col("payload_json.status").cast(StringType()).alias("status"),
                                          F.col("payload_json.transaction_type").cast(StringType()).alias("transaction_type"),
                                          F.col("payload_json.amount").cast(IntegerType()).alias("amount"),
                                          F.col("payload_json.transaction_dt").cast(TimestampType()).alias("transaction_dt"))
    
    if not cur_df_result.rdd.isEmpty():
        try:
            upsert_to_db(cur_df_result, "public.a_merkulov_currencies")
            logger.info(f"Currencies. The transaction was successful!")
    
        except Exception as e:
            handle_exception(e, "Currencies")
    
        finally:
            # Очищаем кэш после записи
            cur_df_result.unpersist()
        
    
    if not trn_df_result.rdd.isEmpty():
        try:
            upsert_to_db(trn_df_result, "public.a_merkulov_transactions")
            logger.info(f"Transactions. The transaction was successful!")
        
        except Exception as e:
            handle_exception(e, "Transactions")
    
        finally:
            # Очищаем кэш после записи
            trn_df_result.unpersist()
            
    result_df.unpersist()

# %%
# Запускаем стриминг
query = (streaming_df.writeStream 
    .trigger(processingTime="15 seconds")
    .outputMode("append")
    .option("truncate", False)
    .foreachBatch(foreach_batch_function)
    .start())

# %%
query.awaitTermination()  
    
while query.isActive:
    print(f"query information: runId={query.runId}, "
          f"status is {query.status}, "
          f"recent progress={query.recentProgress}")
sleep(30)


