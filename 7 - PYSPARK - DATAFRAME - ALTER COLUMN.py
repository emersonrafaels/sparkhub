# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, expr, col, lit, to_timestamp

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

# ADDING A NEW COLUMN
print("ADDING A NEW COLUMN")
df_despachantes.withColumn("new_column", lit(0)).show()

# MODIFYING A NEW COLUMN
print("MODIFYING A NEW COLUMN")
df_despachantes.withColumn("modifying_column", col("vendas") + 10).show()

# REPLACING A EXISTED COLUMN
print("REPLACING A EXISTED COLUMN")
df_despachantes.withColumn("status", lit("não ativo")).show()

# ALTER DATA FORMAT
df_despachantes_alter_data = df_despachantes.withColumn("data2", to_timestamp(col("data"),"yyyy-MM-dd"))
print(df_despachantes_alter_data.schema)