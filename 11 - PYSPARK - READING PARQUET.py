# IMPORTING LIBRARIES
import os

from pyspark.sql import SparkSession

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet File") \
    .getOrCreate()

# Read the Parquet file
df = spark.read.parquet("result_parquet/*")

# Perform operations on the DataFrame
df.show()

# Close the SparkSession
spark.stop()
