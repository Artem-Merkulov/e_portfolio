from datetime import datetime
from logging import Logger
from typing import List, Dict

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from dds_loader.builder import OrderDdsBuilder


class DdsMessageProcessor:
    
    
    def __init__(self,
                consumer: KafkaConsumer,
                producer: KafkaProducer,
                dds_repository: DdsRepository,
                logger: Logger
                ) -> None:
         
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        
        self._logger = logger
        self._batch_size = 30

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now()}: START")

        # Получаем сообщение от KafkaConsumer 
        # Обрабатываем каждое сообщение с помощью OrderDdsBuilder
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            self._logger.info(f"{datetime.now()}: Message received")
            
            # Записываю словарь payload в переменную
            order = msg['payload']
            
            # Готовлю данные для заполнения таблиц
            builder = OrderDdsBuilder(order)
            
            # Заполняю таблицы PostgreSQL             
            self._load_hubs(builder) 
            self._load_links(builder)
            self._load_satellites(builder)
            
            self._logger.info(f"{datetime.now()}. The data has been delivered to PostgreSQL")
            
            
            # Формирую исходящее сообщение для слоя cdm
           
            # Создаём объект dst_msg, который содержит информацию о заказе в формате JSON.
            dst_msg = {
                "object_id": str(builder.h_order().h_order_pk),
                "sent_dttm": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "object_type": "order_report",
                "payload": {
                    "id": str(builder.h_order().h_order_pk),
                    "order_dt": builder.h_order().order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": builder.s_order_status().status,
                    "restaurant": {
                        "id": str(builder.h_restaurant().h_restaurant_pk),
                        "name": builder.s_restaurant_names().name
                    },
                    "user": {
                        "id": str(builder.h_user().h_user_pk),
                        "username": builder.s_user_names().username
                    },
                    "products": self._format_products(builder)
                }
            }
            
            
            self._logger.info(f"{datetime.now()}: {dst_msg}") 
            
            # Отправляю обработанное сообщение в kafka
            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.now()}. Message to Kafka Sent!")
            
            
        self._logger.info(f"{datetime.now()}: FINISH!")
            
        
    # hubs
    # Функция, которая передаёт методы класса OrderDdsBuilder
    # методам класса DdsRepository.
    def _load_hubs(self, builder: OrderDdsBuilder) -> None:   
        
        # Каждый объект category в списке объектов, который мы получаем
        # от метода h_category класса OrderDdsBuilder мы передаём методу
        # h_category_insert класса DdsRepository.
        for category in builder.h_category():
            self._dds_repository.h_category_insert(category)
        # Каждый объект списка, который мы принимаем от метода h_order
        # класса OrderDdsBuilder, мы передаём классу h_order_insert
        # класса DdsRepository.
        self._dds_repository.h_order_insert(builder.h_order())   
        for product in builder.h_product():
            self._dds_repository.h_product_insert(product)     
        self._dds_repository.h_restaurant_insert(builder.h_restaurant())
        self._dds_repository.h_user_insert(builder.h_user())
            
    # links
    def _load_links(self, builder: OrderDdsBuilder) -> None:
        for lop_objects in builder.l_order_product():
            self._dds_repository.l_order_product_insert(lop_objects)
        self._dds_repository.l_order_user_insert(builder.l_order_user())
        for lpc_objects in builder.l_product_category():
            self._dds_repository.l_product_category_insert(lpc_objects)
        for lpr_objects in builder.l_product_restaurant():
            self._dds_repository.l_product_restaurant_insert(lpr_objects)
            
    # satellites
    def _load_satellites(self, builder: OrderDdsBuilder) -> None:
        self._dds_repository.s_order_cost_insert(builder.s_order_cost())
        self._dds_repository.s_order_status_insert(builder.s_order_status())
        for spn_objects in builder.s_product_names():
            self._dds_repository.s_product_names_insert(spn_objects)
        self._dds_repository.s_restaurant_names_insert(builder.s_restaurant_names())
        self._dds_repository.s_user_names_insert(builder.s_user_names())
            
    # Формирование словаря для заполнения поля "products" в dst_msg.   
    # Функция возвращает список словарей.     
    def _format_products(self, builder: OrderDdsBuilder) -> List[Dict]:
        # Переменная в которую записываем словари
        products = []
        # Сюда мы передаём список продуктов с их свойствами из класса s_product_names
        # И для каждого продукта создаём словарь {h_product_pk: name}
        p_names = {x.h_product_pk: x.name for x in builder.s_product_names()}
        # Сюда мы передаём список категорий с их свойствами из класса h_category
        # И для каждой категории создаём словарь {h_category_pk: {"id": h_category_pk, "name": category_name}}
        cat_names = {x.h_category_pk: {"id": str(x.h_category_pk), "name": x.category_name} for x in builder.h_category()}
        # Каждому продукту h_product_pk присваиваем категорию h_category_pk из метода l_product_category
        # Созадём словарь типа {h_product_pk: cat_names[h_category_pk]}
        prod_cats = {x.h_product_pk: cat_names[x.h_category_pk] for x in builder.l_product_category()}
        # Для каждого продукта из списка, который возвращает метод h_product
        # Составляем словарь msg_prod.
        for p in builder.h_product():
            msg_prod = {
                "id": str(p.h_product_pk),
                "name": p_names[p.h_product_pk],
                "category": prod_cats[p.h_product_pk]
            }
            # И добавляем этот словарь в список products
            products.append(msg_prod)
        # Функция возвращает список словарей
        # Попытался воссоздать список, но скорее всего неправильно.
        # {"id": h_product_pk, "name": {h_product_pk: [h_product_pk]}, "category": {h_product_pk: {h_category_pk: {"id": h_category_pk, "name": category_name}}[h_category_pk]}[h_product_pk]}
        return products 