from pyspark.sql import SparkSession
from libs import fake_data

database="database_test"
table="table_test_2"
env_test = True

spark = SparkSession.builder.appName("GlueCatalogIntegration").getOrCreate()

def read_table_teste(database_name: str, table_name: str, spark:SparkSession):
    '''
    '''

    if env_test:
        df = fake_data.generate_dataframe(database, table, spark)
    else: 
        df = spark.table(f"{database}.{table}")

    return df 


def main():
    print("Inicia a leitura de Tabela")
    df = read_table_teste(database, table, spark)

    df.show(truncate=False)
    df.printSchema()


main()