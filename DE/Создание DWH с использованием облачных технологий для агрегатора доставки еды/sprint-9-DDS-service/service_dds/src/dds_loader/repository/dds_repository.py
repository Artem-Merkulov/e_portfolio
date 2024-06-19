# Стандартные библиотеки python
from psycopg2 import extras

# Нестандартные библиотеки python
from lib.pg import PgConnect

# Модули проекта
from dds_loader.dds_entities.dds_entities import H_Category, H_Order, H_Product, H_Restaurant, H_User
from dds_loader.dds_entities.dds_entities import L_Order_User, L_Order_Product, L_Product_Category, L_Product_Restaurant
from dds_loader.dds_entities.dds_entities import S_Order_Cost, S_Order_Status, S_Product_Names, S_Restaurant_Names, S_User_Names

class DdsRepository:
    
    
    # Инициализация подключения к базе данных.
    def __init__(self, db: PgConnect) -> None:

        self._db = db
        
        
    # Добавляем возможность отправить в PostgreSQL данные типа uuid.    
    extras.register_uuid()
        
    # hubs
    
    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_category(
	                    h_category_pk,
	                    category_name,
	                    load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(h_category_pk)s,
                        %(category_name)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (h_category_pk) DO NOTHING
                    ;
                """,          
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }    
                            )   
    
    

    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_order(
                        h_order_pk,
	                    order_id,
	                    order_dt,
                    	load_dt,
                    	load_src
                        )
                    VALUES(
                        %(h_order_pk)s,
                        %(order_id)s,
                        %(order_dt)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (h_order_pk) DO NOTHING
                    ;
                """,          
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }    
                            )
                
    

    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_product(
	                    h_product_pk,
	                    product_id,
	                    load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(h_product_pk)s,
                        %(product_id)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (h_product_pk) DO NOTHING
                    ;
                """,          
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                            )
                
    

    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_restaurant(
	                    h_restaurant_pk,
	                    restaurant_id,
	                    load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (h_restaurant_pk) DO NOTHING
                    ;
                """,          
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                            )
                
    

    def h_user_insert(self, obj: H_User) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_user(
	                    h_user_pk,
	                    user_id,
	                    load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(h_user_pk)s,
                        %(user_id)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (h_user_pk) DO NOTHING
                    ;
                """,          
                    {
                        'h_user_pk': obj.h_user_pk,
                        'user_id': obj.user_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                            )
    
    
    
    # links
                
    def l_order_product_insert(self, obj: L_Order_Product) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_product(
	                    hk_order_product_pk,
	                    h_order_pk,
	                    h_product_pk,
                        load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(hk_order_product_pk)s,
                        %(h_order_pk)s,
                        %(h_product_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (hk_order_product_pk)  DO NOTHING
                    ;
                """,          
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    

    def l_order_user_insert(self, obj: L_Order_User) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_user(
	                    hk_order_user_pk,
	                    h_order_pk,
	                    h_user_pk,
                        load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(hk_order_user_pk)s,
                        %(h_order_pk)s,
                        %(h_user_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (hk_order_user_pk) DO NOTHING
                    ;
                """,          
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                            )
    

    def l_product_category_insert(self, obj: L_Product_Category) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_category(
	                    hk_product_category_pk,
	                    h_product_pk,
	                    h_category_pk,
                        load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(hk_product_category_pk)s,
                        %(h_product_pk)s,
                        %(h_category_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (hk_product_category_pk) DO NOTHING
                    ;
                """,          
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_category_pk': obj.h_category_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src 
                    }
                            )
                

    def l_product_restaurant_insert(self, obj: L_Product_Restaurant) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_restaurant(
	                    hk_product_restaurant_pk,
	                    h_product_pk,
	                    h_restaurant_pk,
                        load_dt,
	                    load_src
	                    )
                    VALUES(
                        %(hk_product_restaurant_pk)s,
                        %(h_product_pk)s,
                        %(h_restaurant_pk)s,
                        %(load_dt)s,
                        %(load_src)s
                        ) 
                    ON CONFLICT (hk_product_restaurant_pk) DO NOTHING
                    ;
                """,          
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                            )
                
    
    
    # satellites

    def s_order_cost_insert(self, obj: S_Order_Cost) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_cost(
                        h_order_pk,
	                    cost,
	                    payment,
                        load_dt,
	                    load_src,
                        hk_order_cost_hashdiff
                        )
                    VALUES(
                        %(h_order_pk)s,
                        %(cost)s,
                        %(payment)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_cost_hashdiff)s
                        ) 
                    ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING
                    ;
                """,          
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                    }
                            )
                
                
        
    def s_order_status_insert(self, obj: S_Order_Status) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_status(
	                    h_order_pk,
	                    status,
                        load_dt,
	                    load_src,
                        hk_order_status_hashdiff
                        )
                    VALUES(
                        %(h_order_pk)s,
                        %(status)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_order_status_hashdiff)s
                        ) 
                    ON CONFLICT (hk_order_status_hashdiff) DO NOTHING
                    ;
                """,          
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                    }
                            )
    
    

    def s_product_names_insert(self, obj: S_Product_Names) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_product_names(
	                    h_product_pk,
	                    name,
                        load_dt,
	                    load_src,
                        hk_product_names_hashdiff
                        )
                    VALUES(
                        %(h_product_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_product_names_hashdiff)s
                        ) 
                    ON CONFLICT (hk_product_names_hashdiff) DO NOTHING
                    ;
                """,          
                    {
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                    }
                            )
    
    

    def s_restaurant_names_insert(self, obj: S_Restaurant_Names) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_restaurant_names(
	                    h_restaurant_pk,
	                    name,
                        load_dt,
	                    load_src,
                        hk_restaurant_names_hashdiff
                        )
                    VALUES(
                        %(h_restaurant_pk)s,
                        %(name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_restaurant_names_hashdiff)s
                        ) 
                    ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING
                    ;
                """,          
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                            )
    
    

    def s_user_names_insert(self, obj: S_User_Names) -> None:
        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_user_names(
                        h_user_pk,
	                    username,
	                    userlogin,
                        load_dt,
	                    load_src,
                        hk_user_names_hashdiff
                        )
                    VALUES(
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s,
                        %(hk_user_names_hashdiff)s
                        ) 
                    ON CONFLICT (hk_user_names_hashdiff) DO NOTHING
                    ;
                """,          
                    {
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                    }
                            )