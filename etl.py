import pandas as pd
import os
import glob
# função que ler e consolida o json 
path = 'data/coleta_dia01.json'

def mesclar_arquivos_json(padrao_diretorio: str) -> pd.DataFrame:
    
    file_paths = glob.glob(padrao_diretorio)

    if not file_paths:
        print(f"Nenhum arquivo encontrado em: {padrao_diretorio}")
        return pd.DataFrame() # para retornar um dataframe vazio 

    df_list = []

    for file_path in file_paths:
        df = pd.read_json(file_path)
        df_list.append(df)

    merged_df = pd.concat(df_list,ignore_index=True)

    return merged_df

# função que transforma 
def kpi_total_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Venda"] * df["Quantidade"]
    return df
 
# funcao que da load em csv ou parquet
def salvar_dados(df: pd.DataFrame, format_saida: list):
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("data/dados.csv", index=False)
            print("Arquivo salvo com sucesso em: data/dados.csv")
        if formato == 'parquet':
            df.to_parquet("data/dados.parquet")
            print("Arquivo salvo com sucesso em: data/dados.csv")

def pipeline_calcular_vendas(caminho_entrada: str, formato_saida: list):
    print("Iniciando processamento dos dados...")
    
    meu_dataframe = mesclar_arquivos_json(caminho_entrada)
    df_total = kpi_total_vendas(meu_dataframe)
    salvar_dados(df_total, formato_saida)
    
    print("Pipeline concluída com sucesso!\n")

if __name__ == "__main__":
    pipeline_calcular_vendas(
        caminho_entrada='data/*.json', 
        formato_saida=["parquet", "csv"]
    )