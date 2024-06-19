# Импорты

# Стандартные библиотеки python
from uuid import UUID
from pydantic import BaseModel


# Определяю классы для таблиц слоя cdm

# marts

class User_Category_Counters(BaseModel):
    user_id: UUID
    category_id: UUID
    category_name: str
    
class User_Product_Counters(BaseModel):
    user_id: UUID
    product_id: UUID
    product_name: str