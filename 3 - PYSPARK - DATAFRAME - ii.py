# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

# CREATING DATAFRAME WITH SCHEMA
schema = "Products STRING, Sales INT"
data_sales = [["Pen", 10], ["Pencil", 20], ["Pen", 40]]

df3 = spark.createDataFrame(data=data_sales, schema=schema)

# VIEW DATAFRAME
print("VIEW DATAFRAME, DEFINED SCHEMA")
df3.show()
df3.printSchema()

# SELECT SOME COLUMNS
print("PRODUCTS:")
df3.select("Products").show()

# GROUPED DATAFRAME
df_group = df3.groupBy("Products").agg(sum("Sales"))
print("VIEWING GROUPED DATAFRAME")
df_group.show()