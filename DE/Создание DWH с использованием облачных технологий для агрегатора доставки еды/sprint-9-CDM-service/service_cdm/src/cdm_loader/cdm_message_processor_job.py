from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.builder import OrderCdmBuilder


# Класс на стадии разработки, поэтому тут присутствуют неиспользуемые переменные.
class CdmMessageProcessor:
    
    
    def __init__(self,

                consumer: KafkaConsumer,
                producer: KafkaProducer,
                cdm_repository: CdmRepository,
                logger: Logger,
                ) -> None:
         
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100


    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now()}: START")
        
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            
            self._logger.info(f"{datetime.now()}: Message received")
            
            # Записываю словарь payload в переменную
            order = msg['payload']
            self._logger.info(f"{datetime.now()}: Payload has been opened!")
            
            # Готовлю данные для заполнения таблиц
            builder = OrderCdmBuilder(order)

            # Вызов функций, которые обновляют таблицы слоя cdm раз в 25 минут
            for ucc_objects in builder.user_category_counters():
                self._cdm_repository.cdm_user_category_insert(ucc_objects)
            for upc_objects in builder.user_product_counters():
                self._cdm_repository.cdm_user_product_insert(upc_objects)
                                                    
            self._logger.info(f"{datetime.now()}. Common data marts has been updated!")
        
        self._logger.info(f"{datetime.now()}: FINISH")
