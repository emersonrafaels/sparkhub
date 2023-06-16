# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession

from etl_functions import Read_Files

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

spark.sql("show databases").show()
spark.sql("create database desp")
spark.sql("use desp").show()
spark.sql("show tables").show()

# READING A DATA
df_despachantes = Read_Files().spark_read_csv(data_dir="DATA/despachantes.csv")

# SAVING DATA TO A HIVE TABLE
df_despachantes.write.mode('overwrite').saveAsTable("TBL_DESPACHANTES")
sql_despachantes = spark.sql("SELECT * FROM TBL_DESPACHANTES")
sql_despachantes.show()