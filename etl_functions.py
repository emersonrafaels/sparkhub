# IMPORTING LIBRARIES
import os
import typing
import logging
from logging.config import fileConfig
from inspect import stack

import pandas as pd
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Emerson\SPARK\Scripts\python.exe"

# CREATING SPARK SESSION
spark = SparkSession.builder.appName("LESSON_PYSPARK_DATAFRAME").getOrCreate()

# INICIALIZANDO O LOGGER
fileConfig('logging_config.ini')
logger = logging.getLogger()

class Read_Files():

    def __init__(self):

        pass

    @staticmethod
    def show_dataframe(df, num_rows_to_display=5):

        """

            FUNÇÃO PARA VISUALIZAR UM DATAFRAME
            FUNCIONA PARA DATAFRAMES PANDAS
            E DATAFRAMES SPARK

            TAMBÉM MOSTRA O TIPAGEM DOS DADOS

            # Arguments
                df                     - Required: Dataframe a ser visualizado (DataFrame)
                num_rows_to_display    - Optional: Quantidade de linhas
                                                   para visualizar (Integer)

        """

        # VERIFICANDO SE O DATAFRAME É PANDAS
        if isinstance(df, pd.DataFrame):

            # DISPLAY CABEÇALHO DOS DADOS
            print(df.head(num_rows_to_display))

            # DISPLAY TIPAGEM DOS DADOS
            print(df.dtypes)

        # VERIFICANDO SE O DATAFRAME É PANDAS
        elif isinstance(df, pyspark.sql.DataFrame):

            # DISPLAY CABEÇALHO DOS DADOS
            print(df.show(num_rows_to_display))

            # DISPLAY TIPAGEM DOS DADOS
            print(df.dtypes)

    @staticmethod
    def pandas_read_excel(data_dir: typing.Union[str, os.PathLike],
                          sheet_name: typing.Union[int, str] = 0,
                          header_num_row: typing.Union[int, None] = 0,
                          names_cols: typing.Union[list, None] = None,
                          columns_to_use: typing.Union[list, None] = None,
                          schema: typing.Union[dict, None] = None) -> pd.DataFrame:

        """

        FUNÇÃO PARA REALIZAR LEITURA DE DADOS
        DE UM EXCEL
        USANDO PANDAS DATAFRAME

        # Arguments
            data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
            sheet_name                  - Optional: Aba do arquivo excel a ser lido (String)
            header_num_row              - Optional: Número da linha a ser considera
                                                    como cabeçalho dos dados (Integer)
            names_cols                  - Optional: Nome das colunas (List)
            columns_to_use              - Optional: Nome das colunas para usar (List)
            schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                    no qual a chave representa o
                                                    nome da coluna e o
                                                    valor seu tipo, respectivamente. (None | StructType)

        # Returns
            df                          - Required: Dados obtidos (Pandas DataFrame)

        """

        # INICIALIZANDO DATAFRAME VAZIO
        df = pd.DataFrame()

        logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO PANDAS EXCEL")

        try:
            df = pd.read_excel(data_dir,
                               sheet_name=sheet_name,
                               header=header_num_row,
                               names=names_cols,
                               usecols=columns_to_use,
                               dtype=schema,
                               engine='openpyxl')

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except:
            try:
                df = pd.read_excel(data_dir,
                                   sheet_name=sheet_name,
                                   header=header_num_row,
                                   names=names_cols,
                                   usecols=columns_to_use,
                                   dtype=schema)

                logger.info("LEITURA REALIZADA COM SUCESSO")

            except Exception as ex:
                logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

        return df

    @staticmethod
    def pandas_read_csv(data_dir: typing.Union[str, os.PathLike],
                        sep: str = ",",
                        encoding: str = 'latin1',
                        header_num_row: typing.Union[int, None] = 0,
                        names_cols: typing.Union[list, None] = None,
                        columns_to_use: typing.Union[list, None] = None,
                        schema: typing.Union[dict, None] = None) -> pd.DataFrame:

        """

        FUNÇÃO PARA REALIZAR LEITURA DE DADOS
        DE UM CSV
        USANDO PANDAS DATAFRAME

        # Arguments
            data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
            sep                         - Optional: Separador dos dados (String)
            encoding                    - Optional: Qual o formato de codificação dos dados.
            header_num_row              - Optional: Número da linha a ser considera
                                                    como cabeçalho dos dados (Integer)
            names_cols                  - Optional: Nome das colunas (List)
            columns_to_use              - Optional: Nome das colunas para usar (List)
            schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                    no qual a chave representa o
                                                    nome da coluna e o
                                                    valor seu tipo, respectivamente.

        # Returns
            df                          - Required: Dados obtidos (Pandas DataFrame)

        """

        # INICIALIZANDO DATAFRAME VAZIO
        df_csv = pd.DataFrame()

        logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO PANDAS CSV")

        try:
            df_csv = pd.read_csv(
                data_dir,
                sep=sep,
                encoding=encoding,
                header='infer',
                skiprows=header_num_row,
                names=names_cols,
                usecols=columns_to_use,
                dtype=schema)

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except Exception as ex:
            logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

        return df_csv

    @staticmethod
    def spark_read_csv(data_dir: typing.Union[str, os.PathLike],
                       header: bool = True,
                       sep: str = ",",
                       encoding: str = "latin1",
                       schema=None):

        """

        FUNÇÃO PARA REALIZAR LEITURA DE DADOS
        DE UM CSV
        USANDO SPARK DATAFRAME

        # Arguments
            data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
            header                      - Optional: Se os dados possuem cabeçalho (Boolean)
            sep                         - Optional: Separador dos dados (String)
            encoding                    - Optional: Qual o formato de codificação dos dados.
            schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                    no qual a chave representa o
                                                    nome da coluna e o
                                                    valor seu tipo, respectivamente.

        # Returns
            df                          - Required: Dados obtidos (Spark DataFrame)

        """

        # INICIALIZANDO DATAFRAME VAZIO
        df_csv = pd.DataFrame()

        logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK CSV")

        try:
            # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
            if schema is None:

                logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

                df_csv = (
                    spark.read.option("delimiter", sep)
                        .option("header", header)
                        .option("encoding", encoding)
                        .csv(data_dir)
                )

            elif isinstance(schema, [StructType, str]):

                logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

                df_csv = (
                    spark.read.option("delimiter", sep)
                    .option("header", header)
                    .option("encoding", encoding)
                    .schema(schema)
                    .csv(data_dir)
                )

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except Exception as ex:
            logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

        return df_csv

    @staticmethod
    def spark_read_parquet(data_dir: typing.Union[str, os.PathLike],
                           header: bool = True,
                           schema=None):

        """

        FUNÇÃO PARA REALIZAR LEITURA DE DADOS
        DE UM PARQUET
        USANDO SPARK DATAFRAME

        # Arguments
            data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
            header                      - Optional: Se os dados possuem cabeçalho (Boolean)
            schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                    no qual a chave representa o
                                                    nome da coluna e o
                                                    valor seu tipo, respectivamente.

        # Returns
            df                          - Required: Dados obtidos (Spark DataFrame)

        """

        # INICIALIZANDO DATAFRAME VAZIO
        df_parquet = pd.DataFrame()

        logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK PARQUET")

        try:
            # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
            if schema is None:

                logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

                df_parquet = (
                    spark.read.option("header", header)
                        .parquet(data_dir)
                )

            elif isinstance(schema, [StructType, str]):

                logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

                df_parquet = (
                    spark.read.option("header", header)
                        .schema(schema)
                        .parquet(data_dir)
                )

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except Exception as ex:
            logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

        return df_parquet

    @staticmethod
    def spark_read_parquet_last_partition(data_dir: typing.Union[str, os.PathLike],
                                          header: bool = True,
                                          schema=None):

        """

        FUNÇÃO PARA REALIZAR LEITURA DE DADOS
        DE UM PARQUET PARTICIONANDO
        USANDO SPARK DATAFRAME

        RETORNA A ÚLTIMA PARTIÇÃO

        # Arguments
            data_dir                    - Required: Diretório do arquivo excel a ser lido (Path)
            header                      - Optional: Se os dados possuem cabeçalho (Boolean)
            schema                      - Optional: Tipagem de cada coluna. É um dict,
                                                    no qual a chave representa o
                                                    nome da coluna e o
                                                    valor seu tipo, respectivamente.

        # Returns
            df                          - Required: Dados obtidos (Spark DataFrame)

        """

        # INICIALIZANDO DATAFRAME VAZIO
        df_parquet = pd.DataFrame()

        logger.info("INICIANDO A LEITURA DOS DADOS UTILIZANDO SPARK PARQUET")

        try:

            # OBTÉM A LISTA DE DIRETÓRIOS DE PARTIÇÃO
            dir_partitions = spark._jvm.org.apache.hadoop.fs.Path(data_dir) \
                .getFileSystem(spark._jsc.hadoopConfiguration()) \
                .listStatus(spark._jvm.org.apache.hadoop.fs.Path(data_dir))

            logger.info("DIRETÓRIOS PARTICIONADOS: {}".format(dir_partitions))

            # ORDENANDO OS DIRETÓRIOS DE PARTIÇÃO PELA ORDEM LEXICOGRÁFICA REVERSA
            dir_partitions = sorted(dir_partitions, key=lambda x: x.getPath().toString(), reverse=True)

            # OBTÉM O PATH PARA A ÚLTIMA PARTIÇÃO
            dir_last_partition = dir_partitions[0].getPath().toString()

            logger.info("DIRETÓRIO DE ÚLTIMA PARTIÇÃO: {}".format(dir_last_partition))

            # VERIFICANDO SE UM SCHEMA FOI DEFINIDO
            if schema is None:

                logger.info("REALIZANDO LEITURA SEM SCHEMA DEFINIDO")

                df_parquet = (
                    spark.read.option("header", header)
                        .parquet(dir_last_partition)
                )

            elif isinstance(schema, [StructType, str]):

                logger.info("REALIZANDO LEITURA COM SCHEMA DEFINIDO: {}".format(schema))

                df_parquet = (
                    spark.read.option("header", header)
                        .schema(schema)
                        .parquet(dir_last_partition)
                )

            logger.info("LEITURA REALIZADA COM SUCESSO")

        except Exception as ex:
            logger.error("ERRO NA FUNÇÃO: {} - {}".format(stack()[0][3], ex))

        return df_parquet


class Save_Files():

    def __init__(self):

        pass

