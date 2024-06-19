# Стандартные библиотеки python
from psycopg2 import extras

# Библиотеки модуля
from lib.pg import PgConnect

# Модули проекта
from cdm_loader.cdm_entities.cdm_entities import User_Category_Counters, User_Product_Counters


class CdmRepository:
    
    # Инициализация подключения к базе данных.
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        
    # Добавляем возможность отправить в PostgreSQL данные типа uuid.    
    extras.register_uuid()

    # Функция, которая заполняет таблицы слоя dds.
    # Раз в 25 секунд витрины очищаются и заполняются данными из слоя dds заново
    def cdm_user_category_insert(self, obj: User_Category_Counters) -> None:
  
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                
                
                # marts
                
                # cdm.user_category_counters
                cur.execute(
                """
                    INSERT INTO cdm.user_category_counters(
                        user_id,
                        category_id,
                        category_name,
                        order_cnt
                        )
                    VALUES(
                        %(user_id)s,
                        %(category_id)s,
                        %(category_name)s,
                        1
                        )
                    ON CONFLICT (user_id, category_id) DO UPDATE SET
                        order_cnt = user_category_counters.order_cnt + 1,
                        category_name = EXCLUDED.category_name
                    ;
                """,
                    {
                        'user_id': obj.user_id,
                        'category_id': obj.category_id,
                        'category_name': obj.category_name
                    }        
                )
                
                
                
    def cdm_user_product_insert(self, obj: User_Product_Counters) -> None:
  
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:        
                # cdm.user_product_counters
                cur.execute(
                """
                    INSERT INTO cdm.user_product_counters(
                        user_id,
                        product_id,
                        product_name,
                        order_cnt
                        )
                    VALUES(
                        %(user_id)s,
                        %(product_id)s,
                        %(product_name)s,
                        1
                        )
                    ON CONFLICT (user_id, product_id) DO UPDATE SET
                        order_cnt = user_product_counters.order_cnt + 1,
                        product_name = EXCLUDED.product_name
                    ;
                """,
                    {
                        'user_id': obj.user_id,
                        'product_id': obj.product_id,
                        'product_name': obj.product_name
                    }
                )