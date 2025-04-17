from pyspark.sql import SparkSession
from libs import fake_data_using_mapping as fd
import sys 
from datetime import datetime, timedelta

# TO-DOs 

# 1 - Verificar se a tabela é iceberg automaticamente   - OK 
# 2 - Adicionar o process_date                          -  
# 3 - Coluna de Partição, é possível identificar? 
# 4 - Overwrite partions 
# 5 - Se não houver o arquivo deve gerar full aleatório 




def is_iceberg_table(response_table: dict) -> bool:
    return response_table['Table']['Parameters']['table_type'] == 'ICEBERG'

def main(table_name: str, database_name: str, config_file : str, num_rows: str, spark:SparkSession):
    
    
    num_rows = int(num_rows)
    # Recuperações necessarias 
    config = fd.read_mapping_json(config_file)
    table_metadata = fd.get_table(database_name, table_name)
    aws_table_fields = fd.treatment_columns(table_metadata)
    iceberg_table = is_iceberg_table(table_metadata)
    
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

    dia_anterior = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    ## Recuperar variaveis pelo ambiente 
    table_name = sys.argv[1]        # Obrigatorio 
    database_name = sys.argv[2]     # Obrigatorio
    partition_by = ""
    overwrite_partition = sys.argv[3] # Obrigatorio

    config_file = sys.argv[3] if len(sys.argv) > 3 else None            # Opcional 
    num_rows = sys.argv[4] if len(sys.argv) > 4    else 10              # Opcional
    process_date = sys.arg[5] if len(sys.argv) > 5 else dia_anterior    # Opcional

   
    main(table_name, database_name, config_file, num_rows, spark)


# Se tiver o arquivo