from libs import fake_data_with_mapping  as fake_wm
from libs import fake_data as fd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType
import random
import re 
from faker import Faker 

spark = SparkSession.builder.appName("GenerateDataFrameFake").getOrCreate() 
faker = Faker() 

# TO-DOs 

# Numeros apenas negativos 
# Numeros com 2 casas decimais 
# Formatacao com zeros a direita
# 

def generate_formatted_data(format_string, num_rows):
    pattern = re.compile(format_string)
    if format_string == "####-##-##":
        return [faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d") for _ in range(num_rows)]
    else:
        return [faker.bothify(format_string) for _ in range(num_rows)]



def generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows):
    
    table_config = config.get(table_name, {})

    def generate_column_data(field_name, field_type):
        if field_name in table_config:
            field_config = table_config[field_name]

            if "is_sequencial" in field_config:
                return list(range(1, num_rows+1))
            elif "choise" in field_config:
                return [random.choice(field_config["choise"]) for _ in range(num_rows)]
            elif "default" in field_config: 
                return [field_config["default"]] * num_rows
            elif "format" in field_config:
                return generate_formatted_data(field_config["format"], num_rows)
            elif "round" in field_config:
                return 

        else:
            if field_type == "bigint":
                return [random.randint(0, 1000) for _ in range(num_rows)]
            elif field_type == "string": 
                return [f"random_{random.randint(1000, 9999)}" for _ in range(num_rows)]
            elif field_type == "double":
                return [random.uniform(1.0, 100.0) for _ in range(num_rows)]
            elif field_type == "float":
                return [random.uniform(1.0, 100.0) for _ in range(num_rows)]
            elif field_type == "boolean":
                return [random.choice([True, False]) for _ in range(num_rows)]
            elif field_type == "date":
                return [faker.date_object() for _ in range(num_rows)]
            elif field_type == "timestamp":
                return [faker.date_time() for _ in range(num_rows)]

            else:
                return [None] * num_rows 
    
    data = {}

    for field in aws_table_fields:
        field_name = field["Name"]
        field_type = field["Type"]
        
        data[field_name] = generate_column_data(field_name, field_type)

    schema = StructType([
        StructField(field["Name"], 
                    IntegerType() if field["Type"] == "bigint" else 
                    DoubleType() if field["Type"] == "double" else 
                    FloatType() if field["Type"] == "float" else 
                    BooleanType() if field["Type"] == "boolean" else 
                    DateType() if field["Type"] == "date" else 
                    TimestampType() if field["Type"] == "timestamp" else 
                    StringType(), 
                    True)
        for field in aws_table_fields
    ])

    rows = list(zip(*data.values()))

    return spark.createDataFrame(rows, schema)



#file_mapping = "table1_test.json"
#table_name = "table1_test"
file_mapping = "table_test_2.json"
table_name = "table_test_2"

database_name = "database_test"
num_rows = 10 

# Recuperações necessarias 
config = fake_wm.read_mapping_json(file_mapping)

table_metadata = fd.get_table(database_name, table_name)

aws_table_fields = fd.treatment_columns(table_metadata)


df = generate_dataframe_with_mapping(aws_table_fields, config, table_name, num_rows)

df.show(truncate=False)