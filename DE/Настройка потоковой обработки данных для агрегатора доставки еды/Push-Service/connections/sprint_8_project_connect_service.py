# В этом файле записаны настройки подключений для приложения sprint-8-project-push-service

# Необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

# Опции безопасности брокера
kafka_security_options = {
    'kafka.bootstrap.servers':'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}

# Опции подключения к БД из которой считываем данные о подписчиках на рестораны
postgres_input_options = {
    'url':'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
    'user': 'student',
    'password': 'de-student'
}

# Опции подключения к БД из которой считываем данные о подписчиках на рестораны
postgres_output_options = {
    'url':'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
    'user': 'jovyan',
    'password': 'jovyan'
}

# Входной и выходной топики брокера
# Данные об акциях из ресторанов (Входящие сообщения)
topik_name_in = 'artem_merkulov_in'
# Данные для push-сервиса (Исходящие сообщения)
topik_name_out = 'artem_merkulov_out'