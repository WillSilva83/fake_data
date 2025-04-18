from pyspark.sql import SparkSession
from libs import fake_data_using_mapping as fd
import sys 
from datetime import datetime, timedelta

# TO-DOs 

# 1 - Adicionar logging - Doing 
# 2 - Tipos de Dados complexos map, array, struct

def partition_of_table(response_table: dict) -> str: 
    partition_keys = response_table['Table'].get('PartitionKeys', [])

    if not partition_keys:
        return ""
    
    return partition_keys[0].get('Name', "")

def is_iceberg_table(response_table: dict) -> bool:
    param = response_table['Table'].get('Parameters', {})
    table_type = param.get('table_type', "NO_ICEBERG")
  
    return table_type == 'ICEBERG'

def main(table_name: str, database_name: str, config_file : str, num_rows: str, spark:SparkSession):

  
    config = fd.read_json(config_file) 
    
    response_table = fd.get_table(database_name, table_name)
    aws_table_fields = fd.treatment_columns(response_table)
    iceberg_table = is_iceberg_table(response_table)
    partition_by = partition_of_table(response_table)
  
    try:
        print("Inicio do processo de FakeData")
        df = fd.generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows, spark)
        df.printSchema()
        df.show(truncate=False)

        # Transformações / Escrita do Dataframe.

    except Exception as e: 
        print(f"Erro ao gerar dados fakes da tabela {table_name}. Erro: {e}")
    


if __name__ == "__main__":

    spark = SparkSession.builder.appName("FakeData").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    dia_anterior = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    ## Recuperar variaveis pelo ambiente 
    table_name = sys.argv[1]          # Obrigatorio 
    database_name = sys.argv[2]       # Obrigatorio
    overwrite_partition = sys.argv[3] # Obrigatorio

    num_rows = int(sys.argv[4]) if len(sys.argv) > 4 else 10              # Opcional
    config_file = sys.argv[5]   if len(sys.argv) > 5 else None            # Opcional
    process_date = sys.argv[6]  if len(sys.argv) > 6 else dia_anterior    # Opcional

    main(table_name, database_name, config_file, num_rows, spark)


# Se tiver o arquivo
