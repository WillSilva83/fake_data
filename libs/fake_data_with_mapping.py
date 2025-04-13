import json 

# Ler o arquivo de mapping 
# Gerar os dados fake com base nesse mapping 
# Verificar o other case/ else do mapping 
# Valor padrão 
# Escrever a partição / Dataframe 


def read_mapping_json(file_path : str) -> dict:
    """
    """
    
    try:
        with open(file_path, 'r') as file: 
            data = json.load(file)
    except FileNotFoundError:
        print("File not Found!")
    except json.JSONDecodeError as e :
        print(f"Error decoding JSON: {e}")

    return data 


    
