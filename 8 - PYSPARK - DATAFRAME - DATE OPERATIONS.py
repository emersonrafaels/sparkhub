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
df_despachantes.show()
df_despachantes.printSchema()

# ALTER DATA FORMAT
print("CREATING DATE COLUMN - FORMAT TIMESTAMP")
df_despachantes_alter_data = df_despachantes.withColumn("date_timestamp", to_timestamp(col("data"),
                                                                                       "dd/MM/yyyy"))
df_despachantes_alter_data.show()
print(df_despachantes_alter_data.schema)

print("VIEW ALL COLUMN DATE TIMESTAMP")
df_despachantes_alter_data.select(year("date_timestamp")).show()

print("VIEW ONLY DISTINCT DATE TIMESTAMP VALUES")
df_despachantes_alter_data.select(year("date_timestamp")).distinct().show()

print("VIEW COLUMNS: NAME AND DATE TIMESTAMP")
df_despachantes_alter_data.select("nome", year("date_timestamp")).orderBy("nome").show()

print("COUNT BY YEAR")
df_despachantes_alter_data.select("date_timestamp").groupBy(year("date_timestamp")).count().show()