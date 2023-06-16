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

# COUNT NUMBER OF ROWS
print("DATAFRAME CONTAINS {} ROWS".format(df_despachantes.count()))

# GROUPBY BY CIDADES (CITIES), AGGREGATE SUM VENDAS (SALES)
groupby_products_sum_vendas = df_despachantes.groupby("cidade").agg(sum("vendas"))

# VIEW GROUPBY RESULT
print("GROUPBY BY CIDADES (CITIES), AGGREGATE SUM VENDAS (SALES)")
groupby_products_sum_vendas.show()

# GETS ALL DATA IN LIST FORMAT
print(df_despachantes.collect())

# ORDER BY VENDAS (SALES)
print("ORDER BY: VENDAS (SALES)")
df_despachantes.orderBy("vendas").show()

# CREATE NEW COLUMN USING SQL - EXPR
df_despachantes.select("cidade", "vendas", expr("vendas*0.25")).show()

# WHERE VENDAS > 20 (SALES > 20)
print("WHERE VENDAS > 20")
df_despachantes.select("id", "nome", "vendas").where(col("vendas") > 20).show()

# WHERE VENDAS > 20 AND VENDAS < 40
print("WHERE VENDAS > 20 AND VENDAS < 40")
df_despachantes.select("id", "nome", "vendas").where((col("vendas") > 20) & (col("vendas") < 40)).show()