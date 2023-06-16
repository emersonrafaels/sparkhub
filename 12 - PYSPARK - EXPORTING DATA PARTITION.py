# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col, lit, to_timestamp, year, count

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
print("VIEW DATAFRAME")
df_despachantes.show()
df_despachantes.printSchema()

# DEFINING PARTITION COLUMN
df_despachantes.groupBy("data").agg(count("*").alias("count")).show()

print("EXPORTING: PARQUET FORMAT")
col_partition = ["data", "status"]
df_despachantes.write.format("parquet").partitionBy(col_partition).mode("overwrite").save("result_parquet")