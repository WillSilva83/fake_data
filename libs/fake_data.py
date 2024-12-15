from datetime import datetime, timedelta
import random 
import boto3
from faker import Faker 
from pyspark.sql.types import (
    StringType, LongType, IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType, StructField, StructType
)

fake = Faker()

def get_table(database_name: str, table_name: str, glue = boto3.client('glue')) -> dict:

    '''
        Retorna o metadado da API Glue. Funcionando localmente.
    '''
    response = {}

    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
    except Exception as e: 
        print(f"Error returning table: {database_name}.{table_name}")

    print(response)
    return response
    
def treatment_columns(response_table: dict):

    '''
        Usada em caso de retorno de API Glue.
    '''

    return response_table['Table']['StorageDescriptor']['Columns']

def map_glue_to_spark_type(glue_type):
    """
    Mapeia os tipos do AWS Glue para os tipos do PySpark.
    """
    # Dicionário de mapeamento
    type_mapping = {
        "bigint": LongType,
        "string": StringType,
        "date": DateType,
        "double": DoubleType,
        "boolean": BooleanType,
        "timestamp": TimestampType,
        "float": FloatType,
        "int": IntegerType  # Adicionado para suporte a tipos inteiros básicos
    }
  
    return type_mapping.get(glue_type, StringType)()

def generate_dataframe(database_name: str, table_name: str, spark):
    
    '''
        Responsavel pela chamada da API Glue e criacao do Dataframe. 

    '''

    # Get Metadata from table 
    response = get_table(database_name, table_name)

    # Treatment Columns
    columns = treatment_columns(response)

    # Schema 
    schema_fields = [
        StructField(col['Name'], map_glue_to_spark_type(col['Type']), True)
        for col in columns
    ]
    # Cria o Schema do DF 
    schema = StructType(schema_fields)

    # Atribui o valor mockado 
    df_spark = generate_mock_data(schema, spark)

    return df_spark
 
def generate_mock_value(data_type):
    
    """
    Gera valores fictícios com base no tipo de dado PySpark.
    """

    # Mapeamento de geradores para cada tipo de dado
    type_generators = {
        IntegerType: lambda: random.randint(1, 1000),
        LongType: lambda: random.randint(1, 1000000),
        FloatType: lambda: round(random.uniform(1.0, 1000.0), 2),
        DoubleType: lambda: round(random.uniform(1.0, 1000000.0), 4),
        StringType: lambda: fake.word(),
        BooleanType: lambda: random.choice([True, False]),
        DateType: lambda: datetime.now().date() - timedelta(days=random.randint(0, 30)),
        TimestampType: lambda: datetime.now() - timedelta(seconds=random.randint(0, 86400)),
    }

    # Procurar o gerador correspondente ao tipo
    for pyspark_type, generator in type_generators.items():
        if isinstance(data_type, pyspark_type):
            return generator()

    # Fallback para tipos não mapeados
    return fake.word()

def generate_mock_data(schema, spark, num_rows=10):
    '''
        Gera o dado fake com base em um numero de linhas, default 10 linhas. 
    '''
    mock_data = []

    for _ in range(num_rows):
        row = [generate_mock_value(field.dataType) for field in schema.fields]
        mock_data.append(row)

    return spark.createDataFrame(mock_data, schema)