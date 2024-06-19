# Импорты

# Стандартные библиотеки
import logging

# Нестандартные библиотеки
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# Модули проекта
from etl.sprint_8_project_flow_enriche import EnricheFlow
from connections.sprint_8_project_spark_session import SparkConnect


# В этом классе реализована основная логика приложения push-service
class MainPushService:
    
    
    # Основная логика приложения
    def main():
        
        # Инициализуем сессию
        spark = SparkConnect.spark_init("RestaurantSubscribeStreamingService")
        logging.info("Сессия открыта успешно!")
        
        # Определяем текущее время в UTC в миллисекундах, затем округляем до секунд
        current_timestamp_utc = (F.round(F.unix_timestamp(F.current_timestamp()))).cast(IntegerType())
    
        # Читаем входящий поток
        input_kafka_stream = EnricheFlow.read_kafka(spark, current_timestamp_utc)
        logging.info("Входящие сообщения kafka поступают!")
    
        # Читаем df с пользователями у которых рестораны в избранном
        input_postgres_df = EnricheFlow.read_subscribers_db(spark)
        logging.info("Данные о подписчиках на рестораны получены!")
    
        # Обогащаем поток
        enriched_flow = EnricheFlow.join_stream_static(input_kafka_stream, input_postgres_df, current_timestamp_utc)
        logging.info("Поток обогащён данными о подписчиках на рестораны!")
        
        # Записываем данные в sinks
        query = EnricheFlow.write_to_kafka_and_pg(enriched_flow)
    
        return query