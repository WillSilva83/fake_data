from faker import Faker 
import random
from datetime import datetime
import json 
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType, DecimalType
import logging
from typing import Optional, Any, List, Dict
import decimal

faker = Faker() 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
NUMERIC_DATATYPES = [
    "bigint", "long", "int", "decimal", "float", "double", "tinyint", "smallint"
]

def treatment_columns(response_table: dict) -> List[Dict[str, Any]]:
    """
    Processa as colunas do descriptor de armazenamento da tabela obtida da API Glue,
    convertendo tipos 'decimal(precision, scale)' em 'decimal'.

    Args:
        response_table (dict): O dicionário contendo os metadados da tabela da API Glue.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários representando as colunas com tipos ajustados.

    Raises:
        KeyError: Se a estrutura esperada em `response_table` estiver ausente.
    """

    try: 
        response = response_table['Table']['StorageDescriptor']['Columns']
        logging.info("Colunas obtidas com sucesso da resposta da API Glue.")

        for column in response:
            if column['Type'].startswith('decimal('):
                column['Type'] = 'decimal'
    
        logging.info("Colunas processadas com sucesso.")
        return response

    except KeyError as error: 
        logging.error(f"Erro ao acessar as colunas: chave ausente na estrutura {error}")
        raise

def get_table(database_name: str, table_name: str, glue_client=None) -> Optional[dict]:
    """
    Retorna o metadado de uma tabela da AWS Glue.

    Args:
        database_name (str): Nome do banco de dados no Glue Data Catalog.
        table_name (str): Nome da tabela no Glue Data Catalog.
        glue_client (boto3.client, optional): Instância do cliente do AWS Glue.
            Se não for fornecido, uma nova instância será criada.

    Returns:
        Optional[dict]: Metadados da tabela como um dicionário, ou None se ocorrer um erro.
    """

    if glue_client is None:
        glue_client = boto3.client('glue')

    logging.info(f"Consultando metadados para a tabela a {database_name}.{table_name}")

    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        logging.info(f"Metadados obtidos com sucesso para a tabela '{table_name}'.")
        return response

    except (ClientError,BotoCoreError) as error:
        logging.error(f"Erro ao obter metadados da tabela '{table_name}': {error}")
        raise

def read_json(file_path: str) -> Optional[dict]:
    """
    Lê um arquivo JSON e retorna os dados como um objeto Python.

    Args:
        file_path (str): Caminho do arquivo JSON a ser lido.

    Returns:
        Optional[dict]: Os dados carregados do JSON como um dicionário ou lista,
        ou None se ocorrer um erro.

    Logs:
        FileNotFoundError: Se o arquivo não for encontrado.
        json.JSONDecodeError: Se o JSON for inválido ou malformado.
    """
    data = None

    if file_path is not None:
        try:
            with open(file_path, 'r') as file: 
                data = json.load(file)
                logging.info(f"Arquivo JSON em '{file_path}' lido com sucesso.")

        except FileNotFoundError:
            logging.error(f"Arquivo '{file_path}' não encontrado.")
            raise
            
        except json.JSONDecodeError as error :
            logging.error(f"Erro ao decodificar o arquivo JSON em '{file_path}': {error}")
            raise
    else:
        logging.info(f"Sem arquivo de Configuração.")
        data = {}
    

    return data 

def generate_data_for_field(
    field_type: str, field_config: Dict[str, Any], row: Dict[str, Any], index: int
) -> Any:
    """
    Gera dados simulados com base no tipo e configuração de um campo.

    Args:
        field_type (str): Tipo do campo (ex.: 'string', 'bigint', 'date').
        field_config (Dict[str, Any]): Configuração específica para o campo.
        row (Dict[str, Any]): Linha atual (útil para campos derivados).
        index (int): Índice da linha atual (útil para valores sequenciais).

    Returns:
        Any: Valor gerado para o campo.

    Raises:
        ValueError: Se o tipo do campo não for suportado.
    """

    def generate_numeric(field_config, field_type):
        """
            Gera um valor numérico baseado no tipo e configurações.
        """
        is_negative = field_config.get("is_negative_number", "False") == "True"
        
        if field_type in ["double", "float"]:
            precision = field_config.get("round", 2)
            base = random.uniform(-1000, -1) if is_negative else random.uniform(0, 1000)
            return round(base, precision)
        
        elif field_type == "decimal":
            precision = field_config.get("round", 2)
            base = random.uniform(-1000, -1) if is_negative else random.uniform(0, 1000)
            return decimal.Decimal(f"{base:.{precision}f}")
        
        elif field_type in ["bigint", "long", "int"]:
            return random.randint(-1000, -1) if is_negative else random.randint(0, 1000)
        
        elif field_config.get("is_sequencial", "False") == "True":
            return index + 1

        else:
            raise ValueError(f"Tipo numérico não suportado: {field_type}")


    # Valores padrão
    if "default" in field_config:
        try:
            if field_type == "date":
                return datetime.strptime(field_config["default"], "%Y-%m-%d").date()
            elif field_type == "timestamp":
                return datetime.fromisoformat(field_config["default"])
            return field_config["default"]
        except Exception as e:
            logging.warning(f"Erro ao processar valor padrão: {e}")
            return None
    
    # Opcoes Pre-definidas 
    elif "choise" in field_config:
        return random.choice(field_config["choise"])
    
    # Valores customizados 
    elif "format" in field_config:
        try:
            if field_config["format"] == "####-##-##":
                return faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
            return faker.bothify(field_config["format"])
        except Exception as e:
            logging.warning(f"Erro ao processar formato customizado: {e}")
            return None
    
    #Campos numericos 
    if field_config.get("is_numeric", "False") == "True" or field_type in NUMERIC_DATATYPES:
        return generate_numeric(field_config, field_config.get("type", field_type))
    
    # Campos derivados 
    if field_config.get("is_derived", "False") == "True":
        derived_field = field_config["derived_field"]
        derived_size = field_config.get("derived_size", 10)
        return str(row.get(derived_field, ""))[:derived_size]

    # Tipos Booleanos 
    if field_type == "boolean":
        return random.choice([True, False])
    
    # Tipos de Data e Timestamp 
    if field_type == "date":
        date_str = faker.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    if field_type == "timestamp":
        timestamp_str = faker.date_time().isoformat()
        return datetime.fromisoformat(timestamp_str)
    
    # Strings aleatorias 
    if field_type == "string":
        return f"random_{random.randint(1000, 9999)}"
    
    logging.warning(f"Tipo de campo não suportado: {field_type}")
    return None


def generate_dataframe_with_mapping(
    aws_table_fields: List[Dict[str, str]],
    config: Dict[str, Dict[str, Any]],
    table_name: str,
    num_rows: int,
    spark: SparkSession
):
    """
    Gera um DataFrame com base no mapeamento de campos e tipos fornecidos.

    Args:
        aws_table_fields (List[Dict[str, str]]): Lista de campos da tabela AWS Glue.
            Exemplo: [{'Name': 'col1', 'Type': 'string'}, {'Name': 'col2', 'Type': 'int'}]
        config (Dict[str, Dict[str, Any]]): Configuração personalizada para a tabela e campos.
        table_name (str): Nome da tabela a ser usada como chave no config.
        num_rows (int): Número de linhas a serem geradas.
        spark (SparkSession): Instância da SparkSession.

    Returns:
        pyspark.sql.DataFrame: DataFrame gerado com os dados simulados.

    Raises:
        ValueError: Se aws_table_fields estiver vazio ou se num_rows for menor que 1.
    """
        # Validação de entrada
    if not aws_table_fields:
        raise ValueError("A lista 'aws_table_fields' não pode estar vazia.")
    if num_rows < 1:
        raise ValueError("O número de linhas 'num_rows' deve ser maior ou igual a 1.")
    
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
        "decimal": DecimalType(12,2), # Necessario ajustar, pensar em algo 
    }

    config_table = config.get(table_name, {})
    rows = []

    for i in range(num_rows):
        row = {}
        for field in aws_table_fields:
            field_name = field['Name']
            field_type = field['Type']
            field_config = config_table.get(field_name, {})

            try:
                row[field_name] = generate_data_for_field(field_type, field_config, row, i)
            except Exception as error: 
                logging.warning(f"Erro ao gerar dado para '{field_name}' do tipo '{field_type}: {error}'")
                row[field_name] = None # Valor em caso de falha 

        rows.append(row)

    schema = StructType([
        StructField(field['Name'], field_types.get(field['Type'], StringType()), True)
        for field in aws_table_fields
    ])

    logging.info(f"DataFrame gerado com {num_rows} linhas para a tabela '{table_name}'.")

    return spark.createDataFrame(rows, schema)

