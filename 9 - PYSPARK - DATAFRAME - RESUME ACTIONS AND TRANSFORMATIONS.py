# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col, lit, to_timestamp, year

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
print("SHOW")
df_despachantes.show()
df_despachantes.printSchema()

print("TAKE: ROW 2")
print(df_despachantes.take(2))

print("COLLECT")
print(df_despachantes.collect())

# ORDER BY
print("ORDER BY - ASCENDING")
df_despachantes.orderBy(col("vendas").asc()).show()

print("ORDER BY - DESCENDING")
df_despachantes.orderBy(col("vendas").desc()).show()

print("ORDER BY - TWO COLUMNS")
df_despachantes.orderBy(col("cidade").desc(), col("vendas").desc()).show()

print("MULTIPLE OPERATIONS IN ONE ROW   ")
df_despachantes.groupBy("cidade").agg(sum("vendas")).orderBy("sum(vendas)").show()