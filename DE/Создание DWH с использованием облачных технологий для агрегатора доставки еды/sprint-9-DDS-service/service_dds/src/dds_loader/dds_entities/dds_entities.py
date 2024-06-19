# Импорты

# Стандартные библиотеки python
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel


# Определяю классы для таблиц слоя dds


# hubs

class H_Category(BaseModel):
    h_category_pk: UUID
    category_name: str
    load_dt: datetime
    load_src: str
    
class H_Order(BaseModel):
    h_order_pk: UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str
    
class H_Product(BaseModel):
    h_product_pk: UUID
    product_id: str
    load_dt: datetime
    load_src: str 
    
class H_Restaurant(BaseModel):
    h_restaurant_pk: UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str
    
class H_User(BaseModel):
    h_user_pk: UUID
    user_id: str
    load_dt: datetime
    load_src: str
    

# links

class L_Order_Product(BaseModel):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime
    load_src: str

class L_Order_User(BaseModel):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime
    load_src: str

class L_Product_Category(BaseModel):
    hk_product_category_pk: UUID
    h_product_pk: UUID
    h_category_pk: UUID
    load_dt: datetime
    load_src: str

class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: UUID
    h_product_pk: UUID
    h_restaurant_pk: UUID
    load_dt: datetime
    load_src: str

# satellites

class S_Order_Cost(BaseModel):
    h_order_pk: UUID
    cost: int
    payment: int
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: UUID

class S_Order_Status(BaseModel):
    h_order_pk: UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: UUID

class S_Product_Names(BaseModel):
    h_product_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: UUID

class S_Restaurant_Names(BaseModel):
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: UUID

class S_User_Names(BaseModel):
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: UUID