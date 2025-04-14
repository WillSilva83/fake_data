from libs import fake_data_with_mapping  as fake_wm
from libs import fake_data as fd
import random
from faker import Faker 
from datetime import datetime


faker = Faker() 

# TO-DOs 

# 1 - Ajustar para ler o JSON e converter os valores corretos - NOK 
# 2 - Campos do tipo decimal - A testar 
# 3 - Código base para testar - main - a fazer 


def generate_data_for_field(field_name: str, field_type: str, field_config, row, index: int):
    """ Gera os dados com base na configuração do campo"""

    def generate_numeric(field_config, field_type):
        is_negative = field_config.get("is_negative_number", "False") == "True"
        
        if field_type in ["double", "float", "decimal", "decimal(18,2)"]:
            precision = int(field_config.get("round", 2))
            base = random.uniform(-1000, -1) if is_negative else random.uniform(0, 1000)
            return round(base, precision)
        
        elif field_type in ["bigint", "long", "int"]:
            return random.randint(-1000, -1) if is_negative else random.randint(0, 1000)
        
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

    elif field_config.get("is_sequencial", "False") == "True":
        return index + 1  # Sequencial gerado com base no índice
    
    elif field_config.get("is_numeric", "False") == "True" or field_type in ["bigint", "long", "int", "decimal(18,2)"]:
        return generate_numeric(field_config, field_config.get("type", field_type))
    
    elif field_config.get("is_derived", "False") == "True":
        derived_field = field_config["derived_field"]
        derived_size = int(field_config.get("derived_size", 10))
        return str(row.get(derived_field, ""))[:derived_size]

    elif "format" in field_config:
        # Gerar dados com bothify para formatos
        if field_config["format"] == "####-##-##":
            return faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        else:
            return faker.bothify(field_config["format"])
    
    elif "choise" in field_config:
        return random.choice(field_config["choise"])

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

def generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows):
    """ Gera um Dataframe com base no mapeamento """
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType

    spark = SparkSession.builder.appName("GenerateDataFrameFake").getOrCreate() 

    field_types = {
        "bigint": LongType(),
        "string": StringType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "decimal(18,2)": DoubleType(),
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
            field_config = config_table.get(field_name, {})
            row[field_name] = generate_data_for_field(
                field_name, field_type, field_config, row, i
            )
        rows.append(row)

    schema = StructType([
        StructField(field['Name'], field_types.get(field['Type'], StringType()), True)
        for field in aws_table_fields
    ])

    return spark.createDataFrame(rows, schema)


file_mapping = "table1_test.json"
table_name = "table1_test"
#file_mapping = "table_test_2.json"
#table_name = "table_test_2"

database_name = ""
num_rows = 1 

# Recuperações necessarias 
config = fake_wm.read_mapping_json(file_mapping)

table_metadata = fd.get_table(database_name, table_name)

aws_table_fields = fd.treatment_columns(table_metadata)


df = generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows)

df.show(truncate=False)

df.printSchema()
