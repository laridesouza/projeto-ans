import pandas as pd
import sqlalchemy
from pathlib import Path

# Configuração da conexão com o SQL Server

engine = sqlalchemy.create_engine(
    r"mssql+pyodbc://@DESKTOP-37BB8EG\SQLEXPRESS01/projetoANS?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
    fast_executemany=True
)

PASTA_DADOS = Path(r"C:\Users\PC\Desktop\projeto-ans\dados")

ENCODING = "latin-1"
SEPARADOR = ";"

#Importa arquivo PLANOS.csv
print("Importando PLANOS.csv...")

df = pd.read_csv(
    PASTA_DADOS / "PLANOS.csv",
    encoding=ENCODING,
    sep=SEPARADOR,
    dtype=str,
    low_memory=False
)

df.to_sql(
    "tb_planos",
    engine,
    if_exists="replace",
    index=False,
    chunksize=1000
)

print("tb_planos criada com sucesso!")

# Controle das Tabelas Hospitalares
controle = {
    "tb_hosp_cons": True,
    "tb_hosp_det": True,
    "tb_hosp_rem": True
}

# Busca todos os csv hospitalares

arquivos = sorted(PASTA_DADOS.glob("**/*HOSP*.csv"))

# Loop de Importação

for arquivo in arquivos:

    try:
        print(f"Lendo {arquivo.name}")

        tipo = arquivo.stem.split("_")[-1]      # tipos: CONS, DET, REM
        tabela = f"tb_hosp_{tipo.lower()}"

        df = pd.read_csv(
            arquivo,
            encoding=ENCODING,
            sep=SEPARADOR,
            dtype=str,
            low_memory=False
        )

        # colunas extras
        df["UF"] = arquivo.stem.split("_")[0]
        df["ANO"] = arquivo.stem.split("_")[1][:4]
        df["MES"] = arquivo.stem.split("_")[1][4:]

        modo = "replace" if controle[tabela] else "append"

        df.to_sql(
            tabela,
            engine,
            if_exists=modo,
            index=False,
            chunksize=1000
        )

        controle[tabela] = False

        print(f"OK: {arquivo.name} -> {tabela}")

    except Exception as erro:
        print(f"ERRO em {arquivo.name}: {erro}")

print("IMPORTAÇÃO FINALIZADA!")