from etl import pipeline_calcular_vendas

if __name__ == "__main__":
    pipeline_calcular_vendas(
        caminho_entrada='data/*.json', 
        formato_saida=["parquet", "csv"]
    )