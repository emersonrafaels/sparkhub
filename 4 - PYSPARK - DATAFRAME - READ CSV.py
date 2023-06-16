# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

# LOAD DATA - DEFINED SCHEMA
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
df_despachantes_with_schema = spark.read.csv("DATA/despachantes.csv",
                                             header=False,
                                             schema=arqschema,
                                             sep=";",
                                             encoding='latin1')

# VIEW DATAFRAME
df_despachantes_with_schema.show()
df_despachantes_with_schema.printSchema()

# LOAD DATA - INFER SCHEMA - SPECIFYING FORMAT CSV
df_despachantes_infer_schema = spark.read.load("DATA/despachantes.csv",
                                               format="csv",
                                               sep=";",
                                               inferSchema=True,
                                               header=False)
df_despachantes_infer_schema.show()
df_despachantes_infer_schema.printSchema()