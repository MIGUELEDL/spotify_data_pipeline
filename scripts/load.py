import os
import sys
import duckdb
from datetime import datetime
from pathlib import Path                        
from dotenv import load_dotenv

load_dotenv()

# Detecta se está rodando dentro do Docker ou local
if os.path.exists("/opt/airflow"):
    BASE_DIR = "/opt/airflow"
    sys.path.append(BASE_DIR)
else:
    BASE_DIR = os.path.abspath(os.path.join('..'))
    sys.path.append(BASE_DIR)

# Importa minio client (autenticação)
from utils.minio_client import MinioClient

# Importa postgrees client (autenticação)
os.environ["POSTGRES_HOST"] = os.getenv("POSTGRES_HOST")
os.environ["POSTGRES_PORT"] = os.getenv("POSTGRES_PORT")

from utils.postgres_client import PostgresClient

#________________________________________________________________________________

BUCKET  = os.getenv("BUCKET_NAME")
BASE_DIR = Path(__file__).resolve().parent.parent
SQL_PATH = BASE_DIR / "sql" / "gold"
SCHEMA  = "gold"

# Cada entrada vira uma tabela no Postgres: gold.<chave>
GOLD_TABLES = {
    "albums_enriched":      "gold_albums_enriched.sql",
    "tracks_enriched":      "gold_tracks_enriched.sql",
    "discografia_summary":  "gold_discografia_summary.sql",
    "evolucao_por_decada":  "gold_evolucao_por_decada.sql",
}

#________________________________________________________________________________

# Helpers

def _ler_sql(filename: str) -> str:
    path = SQL_PATH / filename

    print(f"Lendo SQL: {path}")

    if not path.exists():
        raise FileNotFoundError(f"SQL não encontrado: {path}")

    return path.read_text(encoding="utf-8")

def _carregar_silver(minio: MinioClient, con: duckdb.DuckDBPyConnection) -> None:
    """
    Puxa os Parquet mais recentes da Silver e registra como views no DuckDB.

    get_ultimo_arquivo com formato='parquet' já retorna DataFrame — sem
    conversão extra.
    """
    sources = {
        "silver_albums": "silver/albums_g3",
        "silver_tracks": "silver/tracks_g3",
    }

    for view_name, prefix in sources.items():

        df = minio.get_ultimo_arquivo(BUCKET, prefix, formato="parquet")

        if df is None or df.empty:
            raise ValueError(
                f"Nenhum dado em {BUCKET}/{prefix}. "
                "Rode a Silver antes da Gold."
            )

        # DuckDB recebe um DataFrame pandas e o expõe como view SQL.
        con.register(view_name, df)

def _processar_tabela(
    con: duckdb.DuckDBPyConnection,
    pg: PostgresClient,
    table: str,
    sql_file: str,
) -> int:
    #Executa um SQL no DuckDB e salva o resultado no Postgres.

    #Lê o arquivo .sql como string e converte o resultado em DataFrame
    sql = _ler_sql(sql_file)
    df  = con.execute(sql).df() # .df() = DuckDB Result -> pandas DataFrame
    df  = df.convert_dtypes() # converte tipos

    # Remove horário de colunas de data
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    # salva no Postgres com if_exists='replace'
    pg.write_dataframe(df, table=table, schema=SCHEMA, if_exists="replace")
    return len(df)

#________________________________________________________________________________

# Entrypoint
def run_gold_pipeline() -> dict:
    """
    Executa o pipeline Gold completo.
    Retorna {nome_tabela: linhas} para o XCom do Airflow.
    """
    start = datetime.now()
    minio = MinioClient()
    pg    = PostgresClient()

    # Garante que o schema 'gold' existe no Postgres
    # (na primeira execução ele cria; nas seguintes é no-op)
    pg.create_schema_if_not_exists(SCHEMA)

    # Abre uma conexão DuckDB in-memory.
    con = duckdb.connect(":memory:")

    try:
        # Carrega Silver nas views do DuckDB
        _carregar_silver(minio, con)

        # Processa cada tabela gold
        stats = {}
        for table, sql_file in GOLD_TABLES.items():
            stats[table] = _processar_tabela(con, pg, table, sql_file)

        elapsed = (datetime.now() - start).total_seconds()
            
        print("=" * 55)
        for t, n in stats.items():
            print(f"gold.{t}: {n} linhas")
        print(f"Gold concluído em {elapsed:.1f}s")
        print("=" * 55)

        return stats

    finally:
        # Fecha a conexão DuckDB — as views somem, memória liberada
        con.close()

run_gold_pipeline()