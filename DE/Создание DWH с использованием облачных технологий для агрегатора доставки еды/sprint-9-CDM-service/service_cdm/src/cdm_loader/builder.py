# Импорты

# Стандартные библиотеки python
from uuid import NAMESPACE_DNS
from typing import List, Dict

# Модули проекта
from cdm_loader.cdm_entities.cdm_entities import User_Category_Counters, User_Product_Counters

# Определяем класс OrderDdsBuilder, который служит для создания объектов слоя dds.
class OrderCdmBuilder:
    
    
    # Инициализация класса
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = 'kafka'
        self.order_ns_uuid = NAMESPACE_DNS
        
        
    # Функции для заполнения таблиц
    def user_category_counters(self) -> List[User_Category_Counters]:
        # Выполняю действие только для закрытых заказов
        if self._dict['status'] == 'CLOSED':
        
            category_list = []
        
            user_id = self._dict['user']['id']
            for category_dict in self._dict['products']:
                category_id = category_dict['category']['id']
                category_name = category_dict['category']['name']        
                category_list.append(
                    User_Category_Counters(
                        user_id = user_id,
                        category_id = category_id,
                        category_name = category_name
                    )
                )
            
            return category_list
        
        else:
            
            return []
        
        
        
    def user_product_counters(self) -> List[User_Product_Counters]:
        # Выполняю действие только для закрытых заказов
        if self._dict['status'] == 'CLOSED':
               
            user_id = self._dict['user']['id']
        
            product_list = []
            for product_dict in self._dict['products']:
                product_id = product_dict['id']
                product_name = product_dict['name']
                product_list.append(
                    User_Product_Counters(
                        user_id = user_id,
                        product_id = product_id,
                        product_name = product_name
                    )
                )
                
            return product_list
                
        else: 
            
            return []