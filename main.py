from pyspark.sql import SparkSession
from libs import fake_data_using_mapping as fd
import sys 

def main(table_name: str, database_name: str, config_file : str, num_rows: str, spark:SparkSession):
    
    
    num_rows = int(num_rows)
    # Recuperações necessarias 
    config = fd.read_mapping_json(config_file)
    table_metadata = fd.get_table(database_name, table_name)
    aws_table_fields = fd.treatment_columns(table_metadata)
    
    try: 
        df = fd.generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows, spark)
        df.printSchema()
        df.show(truncate=False)

        # Transformações / Escrita do Dataframe. 

    except Exception as e: 
        print(f"Erro ao gerar dados fakes da tabela {table_name}. Erro: {e}")
    


if __name__ == "__main__":

    spark = SparkSession.builder.appName("FakeData").getOrCreate()

    ## Recuperar variaveis pelo ambiente 
    table_name = sys.argv[1]
    database_name = sys.argv[2]
    config_file = sys.argv[3]
    num_rows = sys.argv[4]
   
    main(table_name, database_name, config_file, num_rows, spark)

