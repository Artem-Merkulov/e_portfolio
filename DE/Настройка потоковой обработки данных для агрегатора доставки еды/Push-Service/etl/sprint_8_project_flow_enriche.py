# Нестандартные библиотеки
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Модули проекта
from connections.sprint_8_project_connect_service import topik_name_in, topik_name_out, kafka_security_options, postgres_input_options,postgres_output_options

# Класс, в котором происходит обогащение потока 
# То есть поток сообщений из Кафки об акциях из ресторанов
# Обогощается данными о пользователях, подписанных на эти рестораны
# А так же происходит запись обогаженного потока в БД Postgres и Kafka
class EnricheFlow:

    # Чтение входящих сообщений из Kafka
    # Формирую формат потока входящих сообщений:
    def read_kafka(spark: SparkSession, current_timestamp_utc) -> DataFrame:
    
    
        # Subscribe to artem_merkulov_in topic
        restaurant_read_stream_df = (spark 
                                          .readStream 
                                          .format("kafka") 
                                          .option("subscribe", topik_name_in) 
                                          .options(**kafka_security_options) 
                                          .load())
        
        # Определяем схему входного сообщения для json
        incomming_message_schema = StructType([
                                StructField('restaurant_id', StringType(), nullable=True),
                                StructField('adv_campaign_id', StringType(), nullable=True),
                                StructField('adv_campaign_content', StringType(), nullable=True),
                                StructField('adv_campaign_owner', StringType(), nullable=True),
                                StructField('adv_campaign_owner_contact', StringType(), nullable=True),
                                StructField('adv_campaign_datetime_start', LongType(), nullable=True),
                                StructField('adv_campaign_datetime_end', LongType(), nullable=True),
                                StructField('datetime_created', LongType(), nullable=True),
                            ])
    
        # Десериализую данные из входящего сообщения JSON
        df_json = (restaurant_read_stream_df.withColumn('key_string', F.col('key').cast(StringType())) 
                                            .withColumn('value_json', F.col('value').cast(StringType())) 
                                            .drop('key', 'value'))
    
        # Раскладываю сообщеня из JSON по полям DataFrame
        df_string = (df_json 
                            .withColumn('key', F.col('key_string')) 
                            .withColumn('value', F.from_json(F.col('value_json'), incomming_message_schema)) 
                            .drop('key_str', 'value_json'))
    
        # Оставляю только нужные для формирования исходящих сообщений поля
        selected_read_stream_df = (df_string.select("value.*"))
    
        # Тут фильтрую по условию: Текущее время должно быть в промеждутке между началом и концом рекламной кампании
        filtered_read_stream_df = (selected_read_stream_df
                                                          .filter((F.col('adv_campaign_datetime_start')<=current_timestamp_utc) & 
                                                                  (F.col('adv_campaign_datetime_end')>=current_timestamp_utc)))
    
        return filtered_read_stream_df
    
            
    
    # Чтение данных о подписчиках на рестораны из Postgres        
    def read_subscribers_db(spark: SparkSession) -> DataFrame:

    
        # Вычитываем всех пользователей с подпиской на рестораны
        subscribers_restaurant_df = (spark.read 
                                                .format('jdbc') 
                                                .options(**postgres_input_options) 
                                                .load())
        # Тут в Postgres у нас есть данные о подписчиках на 2 ресторана, вот id этих ресторанов:
        # 123e4567-e89b-12d3-a456-426614174000 - 9 подписчиков
        # 123e4567-e89b-12d3-a456-426614174001 - 1 подписчик
    
        return subscribers_restaurant_df
    
    
    # Метод, в котором происходит обогащение потока 
    # То есть поток сообщений из Кафки об акциях из ресторанов
    # Обогощается данными о пользователях, подписанных на эти рестораны
    def join_stream_static(filtered_read_stream_df: DataFrame, 
                           subscribers_restaurant_df: DataFrame,
                           current_timestamp_utc) -> DataFrame:

      
        # Джойним данные из сообщений Kafka (информация об акциях в ресторанах) с информацией о подписанных на эти рестораны 
        # пользователях. По полю restaurant_id (uuid). Добавляем время создания события.
        result_df = (filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id')
                                            .withColumn('trigger_datetime_created', current_timestamp_utc)
                                            .drop('id')
                                            .dropDuplicates(['client_id', 'adv_campaign_id'])
                                            .select('restaurant_id',
                                                    'adv_campaign_id',
                                                    'adv_campaign_content',
                                                    'adv_campaign_owner',
                                                    'adv_campaign_owner_contact',
                                                    'adv_campaign_datetime_start',
                                                    'adv_campaign_datetime_end',
                                                    'client_id',
                                                    'datetime_created',
                                                    'trigger_datetime_created'))
    
        return result_df
            
            
    # Функция для обработки microbatches
    def foreach_batch_function(result_df: DataFrame, epoch_id) -> DataFrame:
    
        # Сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
        result_df.persist()
    
        # Записываем df в PostgreSQL с полем feedback
        (result_df.withColumn('feedback', F.lit(None).cast(StringType()))
                  .write 
                  .mode("append") 
                  .format("jdbc") 
                  .options(**postgres_output_options)
                  .save())
    
        # Сериализую сообщения для отправки в kafka
        kafka_df = (result_df
                             .select(F.to_json(F.struct(F.col('*'))).alias('value'))
                             .select('value'))
        
        # Отправляем сообщения в результирующий топик Kafka без поля feedback
        (kafka_df.write
                       .format('kafka')
                       .options(**kafka_security_options) 
                       .option('topic', topik_name_out)
                       .save())
    
        # Очищаем память от df
        result_df.unpersist()
    
    
    # Метод для записи данных в PostgreSQL для аналитики обратной связи от пользователей
    # И для записи обогащённых данных в выходной топик Kafka
    def write_to_kafka_and_pg(enriched_flow):
        
        query = (enriched_flow.writeStream 
                                        .foreachBatch(EnricheFlow.foreach_batch_function) 
                                        .trigger(processingTime="15 seconds")
                                        .option("truncate", False)
                                        .start())
        return query