# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING SPARK SESSION
spark = SparkSession.builder.appName('LESSON_PYSPARK_DATAFRAME').getOrCreate()

# CREATING DATAFRAME, WITHOUT DEFINED SCHEMA
df1 = spark.createDataFrame([("Pedro", 10), ("Maria", 20), ("José", 40)])

# USING ACTION SHOW, TO EXECUTE PREVIOUS STEPS
# SPARK HAS LAZY ARCHITECTURE
print("VIEW DATAFRAME, INFER SCHEMA")
df1.show()
df1.printSchema()

# CREATING DATAFRAME WITH SCHEMA
schema = "Id INT, Name STRING"
dados = [[1, "Pedro"], [2, "Maria"]]
df2 = spark.createDataFrame(dados, schema)

# VIEW DATAFRAME
print("VIEW DATAFRAME, DEFINED SCHEMA")
df2.show()
df2.printSchema()

# VIEW ONLY ONE ROW
df2.show(1)