# Импорты

# Стандартные библиотеки python
from uuid import UUID, NAMESPACE_DNS, uuid5
from datetime import datetime
from typing import List, Dict, Any

# Модули проекта
from dds_loader.dds_entities.dds_entities import H_User, H_Product, H_Category, H_Order, H_Restaurant
from dds_loader.dds_entities.dds_entities import L_Order_Product, L_Order_User, L_Product_Category, L_Product_Restaurant
from dds_loader.dds_entities.dds_entities import S_User_Names, S_Order_Cost, S_Order_Status, S_Product_Names, S_Restaurant_Names



# Определяем класс OrderDdsBuilder, который служит для создания объектов слоя dds.
class OrderDdsBuilder:
    
    # Инициализация класса
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = 'kafka'
        self.order_ns_uuid = NAMESPACE_DNS
        
    # Функции для приведения значений к типу uuid.
    def _uuid(self, obj: Any) -> UUID:
        return uuid5(namespace=self.order_ns_uuid, name=str(obj))  
    
    
    
    # Функции для заполнения таблиц
    

    # hubs
 
    # В этой функции мы получаем список объектов класса H_Category
    # (с присущими ему свойствами h_product_pk
    #                             category_name
    #                             load_dt
    #                             load_src)
    # Т.е. список категорий, но с указанными свойствами.
    # Для каждого продукта в заказе пользователя.
    def h_category(self) -> List[H_Category]:
        # Список категорий
        categories = []
        # Для каждого продукта в заказе клиента 
        for prod_dict in self._dict['products']:
            # Определяем название категории к которой относится продукт
            category_name = prod_dict['category']
            # Определяем свойства категорий 
            # И добавляем объект H_Category в список
            # То есть получаем список списков.
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.now(),
                    load_src=self.source_system
                )
            )

        return categories     
    


    def h_order(self) -> List[H_Order]:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.now(),
            load_src=self.source_system
        )    
    


    def h_product(self) -> List[H_Product]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.now(),
                    load_src=self.source_system
                )
            )

        return products 
    


    def h_restaurant(self) -> List[H_Restaurant]:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.now(),
            load_src=self.source_system
        )    




    def h_user(self) -> List[H_User]:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.now(),
            load_src=self.source_system
        )    
        


    # links
                
    def l_order_product(self) -> List[L_Order_Product]:
        # Определяем список в который будем добавлять объекты класса L_Order_Product
        op_list = []
        # Определяем переменную order_id
        order_id = self._dict['id']
        # Для каждого продукта в заказе пользователя формируем объект класса L_Order_Product  
        # И добавляем его в список o_p_link 
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            op_list.append(
                L_Order_Product(
                    hk_order_product_pk = self._uuid((str(order_id)+prod_id)),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.now(),
                    load_src=self.source_system
                )  
            )
        
        return op_list 

    
    
    def l_order_user(self) -> List[L_Order_User]:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return L_Order_User(
            hk_order_user_pk = self._uuid((str(order_id)+user_id)),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.now(),
            load_src=self.source_system
        )    
        
        
        
    def l_product_category(self) -> List[L_Product_Category]:
        
        pc_list = []
            
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            category_name = prod_dict['category']
            pc_list.append(
                L_Product_Category(
                    hk_product_category_pk = self._uuid((prod_id+category_name)),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(category_name),
                    load_dt=datetime.now(),
                    load_src=self.source_system
                )  
            ) 
        
        return pc_list 
        
        
        
    def l_product_restaurant(self) -> List[L_Product_Restaurant]:
        
        pr_list = []
        
        restaurant_id = self._dict['restaurant']['id']
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            pr_list.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk = self._uuid((prod_id+restaurant_id)),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.now(),
                    load_src=self.source_system
                ) 
            )
        
        return pr_list
        
        
    
    # satellits
    
    def s_order_cost(self) -> List[S_Order_Cost]:
        order_id = self._dict['id']
        cost = self._dict['cost']
        payment = self._dict['payment']
        return S_Order_Cost(
            h_order_pk=self._uuid(order_id),
            cost=cost,
            payment=payment,
            load_dt=datetime.now(),
            load_src=self.source_system,
            hk_order_cost_hashdiff = self._uuid((str(order_id)+str(cost)))
        )        
    
    
    
    def s_order_status(self) -> List[S_Order_Status]:
        order_id = self._dict['id']
        status = self._dict['status']
        return S_Order_Status(
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=datetime.now(),
            load_src=self.source_system,
            hk_order_status_hashdiff = self._uuid((str(order_id)+status))
        )     
        
        
        
    def s_product_names(self) -> List[S_Product_Names]:
        
        pn_list = []
        
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            name = prod_dict['name']
            pn_list.append(
                S_Product_Names(
                    h_product_pk=self._uuid(prod_id),
                    name=name,
                    load_dt=datetime.now(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff = self._uuid((prod_id+name))
                )  
            )
            
        return pn_list


    def s_restaurant_names(self) -> S_Restaurant_Names:
        restaurant_id = self._dict['restaurant']['id']
        name = self._dict['restaurant']['name']
        return S_Restaurant_Names(
            h_restaurant_pk=self._uuid(restaurant_id),
            name=name,
            load_dt=datetime.now(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff = self._uuid((restaurant_id+name))
        )  
        
        

    def s_user_names(self) -> S_User_Names:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return S_User_Names(
            h_user_pk=self._uuid(user_id),
            username=username,
            userlogin=userlogin,
            load_dt=datetime.now(),
            load_src=self.source_system,
            hk_user_names_hashdiff = self._uuid((user_id+username))
        ) 