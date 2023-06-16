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

# Define o caminho base do diretório particionado
caminho_base = "result_parquet"

# Obtém a lista de diretórios de partição
diretorios_particao = spark._jvm.org.apache.hadoop.fs.Path(caminho_base)\
    .getFileSystem(spark._jsc.hadoopConfiguration())\
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(caminho_base))

# Ordena os diretórios de partição pela ordem lexicográfica reversa
diretorios_particao = sorted(diretorios_particao, key=lambda x: x.getPath().toString(), reverse=True)

# Obtém o caminho da última partição
ultimo_diretorio_particao = diretorios_particao[0].getPath().toString()

print(ultimo_diretorio_particao)

# Lê o arquivo Parquet da última partição
df = spark.read.parquet(ultimo_diretorio_particao)

# Exemplo: Imprimir as primeiras linhas do DataFrame
df.show()