from etl_functions import Read_Files

data_dir_excel = "DATA/BASE_COM_CEP.xlsx"
data_dir_csv = "DATA/despachantes.csv"
data_dir_parquet = "result_parquet/data=01%2F12%2F2022"
data_dir_parquet_partitions = "result_parquet/"

# INICIALIZANDO A CLASSE DO READ FILES
read_files_functions = Read_Files()

# REALIZANDO A LEITURA COM PANDAS EXCEL
df_pd_excel = read_files_functions.pandas_read_excel(data_dir_excel)
read_files_functions.show_dataframe(df=df_pd_excel, num_rows_to_display=5)
print("-"*50)

# REALIZANDO A LEITURA COM PANDAS CSV
df_pd_csv = read_files_functions.pandas_read_csv(data_dir_csv, sep=";")
read_files_functions.show_dataframe(df=df_pd_csv, num_rows_to_display=7)
print("-"*50)

# REALIZANDO A LEITURA COM SPARK CSV
df_spark_csv = read_files_functions.spark_read_csv(data_dir_csv, sep=";")
read_files_functions.show_dataframe(df=df_spark_csv, num_rows_to_display=10)
print("-"*50)

# REALIZANDO A LEITURA COM SPARK PARQUET
df_spark_parquet = read_files_functions.spark_read_parquet(data_dir_parquet)
read_files_functions.show_dataframe(df=df_spark_parquet, num_rows_to_display=10)
print("-"*50)

# REALIZANDO A LEITURA DA ÚLTIMA PARTIÇÃO COM SPARK PARQUET
df_spark_parquet_last_partiton = read_files_functions.spark_read_parquet_last_partition(data_dir_parquet_partitions)
read_files_functions.show_dataframe(df=df_spark_parquet_last_partiton, num_rows_to_display=10)
print("-"*50)