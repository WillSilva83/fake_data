# Dados Mockados Para Desenvolvimento 

## Pré-Requisitos 

Modulo da AWS para retorno das tabelas usando API do Glue. 
```sh 
pip install boto3 
```

Modulo python para geração de dados faker.
```sh 
pip install faker
```

## Como realizar a chamada do Faker

```python 
## Importação do modulo no caso esta em um diretorio de libs 
from libs import fake_data

'''
    Faça a chamada com base no fake_data para gerar um dataframe. 
    
    database : str - Database de Origem da Tabela 
    table : str - Tabela de origem dos dados
    spark : SparkSession - Sessao spark atual 
'''

spark = SparkSession.builder.appName("GlueCatalogIntegration").getOrCreate()

df = fake_data.generate_dataframe(database, table, spark)

``` 
