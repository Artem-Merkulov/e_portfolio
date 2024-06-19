# Нестандартные библиотеки
from pyspark.sql import SparkSession

# Модули проекта
from connections.sprint_8_project_connect_service import spark_jars_packages


# В этом классе происходит подключение к Spark и инициализация Spark-Сессии
class SparkConnect:
    
    
    def spark_init(app_name) -> SparkSession:
    
    
        # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
        spark = (SparkSession.builder 
                                     .appName(app_name) 
                                     .config("spark.sql.session.timeZone", "UTC") 
                                     .config("spark.jars.packages", spark_jars_packages) 
                                     .getOrCreate())

        return spark