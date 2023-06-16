# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

# LOAD DATA - DEFINED SCHEMA
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
df_despachantes = spark.read.csv("DATA/despachantes.csv",
                                 header=False,
                                 schema=arqschema,
                                 sep=";",
                                 encoding='latin1')

# VIEW DATAFRAME
df_despachantes.show()
df_despachantes.printSchema()

# RENAME ONE COLUMN
df_despachantes_rename = df_despachantes.withColumnRenamed("nome", "nomes")

# VIEW DATAFRAME
print("VIEW ONE COLUMN RENAME")
df_despachantes_rename.show()
df_despachantes_rename.printSchema()

# RENAME MULTIPLE COLUMNS
df_despachantes_multiple_renames = df_despachantes.withColumnRenamed("nome", "nomes").withColumnRenamed("cidade", "cidades")

# VIEW DATAFRAME
print("VIEW MULTIPLE RENAMES")
df_despachantes_multiple_renames.show()
df_despachantes_multiple_renames.printSchema()