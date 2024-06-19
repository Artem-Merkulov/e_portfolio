#!/usr/bin/env python
# coding: utf-8

# In[9]:


# Импорты
from numpy.random import choice, randint
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError


# In[17]:


# Generation of the dummy data
def get_random_value():
    new_dict = {}

    restaurant_id_list = ['123e4567-e89b-12d3-a456-426614174000', 
                          '200e4567-e89b-12d3-a456-426614174001']
    adv_campaign_id_list = ['111e4567-e89b-12d3-a456-426614174003', 
                            '222e4567-e89b-12d3-a456-426614174003', 
                            '333e4567-e89b-12d3-a456-426614174003', 
                            '444e4567-e89b-12d3-a456-426614174003']
    adv_campaign_content_list = ['first campaign', 
                                 'second campaign', 
                                 'third campaign', 
                                 'fourth campaign']
    adv_campaign_owner_list = ['Ivanov Ivan Ivanovich', 
                               'Petrov Petr Petrovich', 
                               'Lyvov Lev Lyvovich', 
                               'Semenov Semen Semenovich']
    adv_campaign_owner_contact_list = ['iiivanov@restaurant_id', 
                                       'pppetrov@restaurant_id', 
                                       'lllyvov@restaurant_id', 
                                       'sssemenov@restaurant_id']
    adv_campaign_datetime_start_list = 1659203516
    adv_campaign_datetime_end = 2659207116

    new_dict['restaurant_id'] = choice(restaurant_id_list)
    new_dict['adv_campaign_id'] = choice(adv_campaign_id_list)
    new_dict['adv_campaign_content'] = choice(adv_campaign_content_list)
    new_dict['adv_campaign_owner'] = choice(adv_campaign_owner_list)
    new_dict['adv_campaign_owner_contact'] = choice(adv_campaign_owner_contact_list)
    new_dict['adv_campaign_datetime_start'] = adv_campaign_datetime_start_list
    new_dict['adv_campaign_datetime_end'] = adv_campaign_datetime_end
    new_dict['datetime_created'] = randint(1659131516, 2659207116)

    return new_dict


# In[18]:


# Запуск генератора
if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'],
                         security_protocol='SASL_SSL',
                         sasl_mechanism='SCRAM-SHA-512',
                         ssl_cafile='/usr/local/share/ca-certificates/Yandex/YandexCA.crt',
                         sasl_plain_username="de-student",
                         sasl_plain_password="ltcneltyn",
                         value_serializer=lambda x:dumps(x).encode('utf-8'),
                         compression_type='gzip')
    my_topic = 'artem_merkulov_in'
   
   
    while True:  
        for _ in range(100):
            data = get_random_value()

            try:
                future = producer.send(topic = my_topic, value = data)
                record_metadata = future.get(timeout=10)
                
                print('--> The message has been sent to a topic: {}, partition: {}, offset: {}' \
                        .format(record_metadata.topic, 
                            record_metadata.partition, 
                            record_metadata.offset ))   
                                         
            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))

            finally:
                producer.flush()

        sleep(1)

    producer.close()

