from faker import Faker 
import random
from datetime import datetime
import json 
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType

faker = Faker() 



def treatment_columns(response_table: dict):

    '''
        Usada em caso de retorno de API Glue.
    '''

    response = response_table['Table']['StorageDescriptor']['Columns']

    for column in response:
        if column['Type'].startswith('decimal('):
            column['Type'] = 'decimal'

    return response

def get_table(database_name: str, table_name: str, glue = boto3.client('glue')) -> dict:

    '''
        Retorna o metadado da API Glue. Funcionando localmente.
    '''
    response = {}

    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
    except Exception as e: 
        print(f"Error returning table: {database_name}.{table_name}")

    return response

def read_mapping_json(file_path : str) -> dict:
    """
        Função para para leitura do JSON. 
    """
    try:
        with open(file_path, 'r') as file: 
            data = json.load(file)
    except FileNotFoundError:
        print(f"File {file_path} not Found!")
    except json.JSONDecodeError as e :
        print(f"Error decoding JSON: {e}")

    return data 

def generate_data_for_field(field_type: str, field_config, row, index: int):
    """ Gera os dados com base na configuração do campo"""

    numeric_datatypes = ["bigint", "long", "int", "decimal", "float", "double", "tinyint", "smallint"]
    

    def generate_numeric(field_config, field_type):
        is_negative = field_config.get("is_negative_number", "False") == "True"
        
        if field_type in ["double", "float", "decimal"]:
            precision = field_config.get("round", 2)
            base = random.uniform(-1000, -1) if is_negative else random.uniform(0, 1000)
            return round(base, precision)
        
        elif field_type in ["bigint", "long", "int"]:
            return random.randint(-1000, -1) if is_negative else random.randint(0, 1000)
        
        elif field_config.get("is_sequencial", "False") == "True":
            return index + 1

        else:
            raise ValueError(f"Unsupported numeric type: {field_type}")


    if "default" in field_config:
        if field_type == "date":
            date_str = field_config["default"]
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        elif field_type == "timestamp":
            timestamp_str = field_config["default"]
            return datetime.fromisoformat(timestamp_str)
        else:
            return field_config["default"]
    
    elif "choise" in field_config:
        return random.choice(field_config["choise"])
    
    elif "format" in field_config:
        if field_config["format"] == "####-##-##":
            return faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        else:
            return faker.bothify(field_config["format"])
    
    elif field_config.get("is_numeric", "False") == "True" or field_type in numeric_datatypes:
        return generate_numeric(field_config, field_config.get("type", field_type))
    
    elif field_config.get("is_derived", "False") == "True":
        derived_field = field_config["derived_field"]
        derived_size = field_config.get("derived_size", 10)
        return str(row.get(derived_field, ""))[:derived_size]


    elif field_type == "boolean":
        return random.choice([True, False])
    
    elif field_type == "date":
        date_str = faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    
    elif field_type == "timestamp":
        timestamp_str = faker.date_time().isoformat()
        return datetime.fromisoformat(timestamp_str)
    
    elif field_type == "string":
        return f"random_{random.randint(1000, 9999)}"
    


    else:
        return None

def generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows, spark:SparkSession):
    """ Gera um Dataframe com base no mapeamento """
    
    field_types = {
        "bigint": LongType(),
        "string": StringType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "int": LongType(),
        "long": LongType(),
        "decimal": DoubleType(),
    }


    config_table = config.get(table_name, {})
    rows = []

    for i in range(num_rows):
        row = {}
        for field in aws_table_fields:
            field_name = field['Name']
            
            field_type = field['Type']

            # caso tenha decimal com casas decimais
            if 'decimal' in field_type:
               field_type = 'decimal'

            field_config = config_table.get(field_name, {})
            row[field_name] = generate_data_for_field(
                field_type, field_config, row, i
            )
        rows.append(row)

    schema = StructType([
        StructField(field['Name'], field_types.get(field['Type'], StringType()), True)
        for field in aws_table_fields
    ])

    return spark.createDataFrame(rows, schema)

