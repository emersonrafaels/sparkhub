# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from etl_functions import Read_Files

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

# SHOW ALL DATABASES WE HAVE
spark.sql("show databases").show()

# CREATE NEW DATABASE
spark.sql("create database desp")
spark.sql("use desp")

# SHOW ALL DATABASES WE HAVE AGAIN
spark.sql("show databases").show()

# READING A DATA
df_despachantes = Read_Files().spark_read_csv(data_dir="DATA/despachantes.csv")

# SAVING DATA TO A HIVE TABLE
df_despachantes.write.saveAsTable("TBL_DESPACHANTES")

# PRINTING THE TABLES
spark.sql("show tables").show()