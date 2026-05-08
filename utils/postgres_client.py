import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class PostgresClient:

    def __init__(self):
        # Credenciais lidas do .env
        self.host     = os.getenv("POSTGRES_HOST")
        self.port     = os.getenv("POSTGRES_PORT")
        self.dbname   = os.getenv("POSTGRES_DB")
        self.user     = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")

    def _conectar(self):
        # Abre e retorna uma conexão com o Postgres
        return psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname,
            user=self.user, password=self.password,
        )

    def create_schema_if_not_exists(self, schema: str) -> None:
        # Cria o schema no Postgres se ainda não existir
        with self._conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    def write_dataframe(self, df: pd.DataFrame, table: str, schema: str = "gold", if_exists: str = "replace", chunksize: int = 500) -> None:
        with self._conectar() as conn:
            with conn.cursor() as cur:

                # Recria a tabela do zero se if_exists='replace'
                if if_exists == "replace":
                    cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')

                # Define o tipo SQL de cada coluna com base no dtype do DataFrame
                tipos = {"int": "BIGINT", "float": "FLOAT", "bool": "BOOLEAN"}
                colunas = [
                    f'"{col}" {next((t for k, t in tipos.items() if k in str(dtype)), "TEXT")}'
                    for col, dtype in df.dtypes.items()
                ]

                # Cria a tabela com as colunas mapeadas
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({", ".join(colunas)})')

                # Converte tipos numpy pra Python nativo que o psycopg2 aceita
                rows = [
                    tuple(v.item() if hasattr(v, 'item') else v for v in r)
                    for r in df.itertuples(index=False, name=None)
                ]

                # Insere os dados em lotes
                cols = ", ".join(f'"{c}"' for c in df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                for i in range(0, len(rows), chunksize):
                    cur.executemany(f'INSERT INTO "{schema}"."{table}" ({cols}) VALUES ({placeholders})', rows[i:i+chunksize])

        print(f"✅ {len(df)} linhas → {schema}.{table}")

    def read_sql(self, query: str) -> pd.DataFrame:
        # Executa uma query e retorna o resultado como DataFrame
        with self._conectar() as conn:
            return pd.read_sql(query, conn)

    def read_table(self, table: str, schema: str = "gold") -> pd.DataFrame:
        # Busca uma tabela inteira do schema gold
        return self.read_sql(f'SELECT * FROM "{schema}"."{table}"')