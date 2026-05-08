import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import duckdb                        
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).parent.parent))
from utils.minio_client import MinioClient
from utils.postgres_client import PostgresClient

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("gold.load")

BUCKET  = os.getenv("BUCKET_NAME", "spotify-data")
SQL_DIR = Path(__file__).parent.parent / "sql" / "gold"
SCHEMA  = "gold"

# Cada entrada vira uma tabela no Postgres: golds
GOLD_TABLES = {
    "albums_enriched":      "gold_albums_enriched.sql",
    "tracks_enriched":      "gold_tracks_enriched.sql",
    "discografia_summary":  "gold_discografia_summary.sql",
    "evolucao_por_decada":  "gold_evolucao_por_decada.sql",
}


# ── Helpers ──────────────────────────────────────────────────────────

def _ler_sql(filename: str) -> str:
    """Lê um arquivo .sql do disco e retorna o conteúdo como string."""
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL não encontrado: {path}")
    return path.read_text(encoding="utf-8")


def _carregar_silver(minio: MinioClient, con: duckdb.DuckDBPyConnection) -> None:
    """
    get_ultimo_arquivo com formato='parquet' já retorna DataFrame — sem
    conversão extra, o MinioClient cuida disso.
    """
    sources = {
        "silver_albums": "silver/albums_g3",
        "silver_tracks": "silver/tracks_g3",
    }

    for view_name, prefix in sources.items():
        logger.info("Silver: %s/%s", BUCKET, prefix)

        df = minio.get_ultimo_arquivo(BUCKET, prefix, formato="parquet")

        if df is None or df.empty:
            raise ValueError(
                f"Nenhum dado em {BUCKET}/{prefix}. "
                "Rode a Silver antes da Gold."
            )

        # Aqui está a mágica: o DuckDB recebe um DataFrame pandas
        # e o expõe como view SQL com o nome que você escolher.
        con.register(view_name, df)

        logger.info(" View '%s': %d linhas × %d colunas",
                    view_name, len(df), len(df.columns))


def _processar_tabela(
    con: duckdb.DuckDBPyConnection,
    pg: PostgresClient,
    table: str,
    sql_file: str,
) -> int:
    """
    Executa um SQL no DuckDB e salva o resultado no Postgres.

    Passo a passo:
      1. Lê o arquivo .sql como string
      2. con.execute(sql) → roda o SQL sobre as views registradas
      3. .df()           → converte o resultado em DataFrame pandas
      4. pg.write_dataframe() → salva no Postgres com if_exists='replace'
         (recria a tabela inteira a cada run — correto pra batch diário)
    """
    logger.info("Processando: gold.%s", table)

    sql = _ler_sql(sql_file)
    df  = con.execute(sql).df()       # .df() = DuckDB Result → pandas DataFrame

    logger.info("%d linhas geradas", len(df))
    pg.write_dataframe(df, table=table, schema=SCHEMA, if_exists="replace")
    return len(df)

# ── Entrypoint ───────────────────────────────────────────────────────

def run_gold_pipeline() -> dict:
    """
    Executa o pipeline Gold completo.
    Retorna {nome_tabela: linhas} para o XCom do Airflow.
    """
    start = datetime.now()
    logger.info("=" * 55)
    logger.info("GOLD — %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 55)

    minio = MinioClient()
    pg    = PostgresClient()

    # Garante que o schema 'gold' existe no Postgres
    # (na primeira execução ele cria; nas seguintes é no-op)
    pg.create_schema_if_not_exists(SCHEMA)

    # Abre uma conexão DuckDB in-memory.
    # Tudo que acontece aqui vive só na memória RAM — nenhum arquivo
    # .duckdb é criado em disco. É intencional: queremos uma engine
    # de transformação limpa a cada execução.
    con = duckdb.connect(":memory:")

    try:
        # Carrega Silver nas views do DuckDB
        _carregar_silver(minio, con)

        # Processa cada tabela gold
        stats = {}
        for table, sql_file in GOLD_TABLES.items():
            stats[table] = _processar_tabela(con, pg, table, sql_file)

        elapsed = (datetime.now() - start).total_seconds()
        logger.info("=" * 55)
        logger.info("GOLD concluído em %.1fs", elapsed)
        for t, n in stats.items():
            logger.info("gold.%-30s %d linhas", t, n)
        logger.info("=" * 55)

        return stats

    finally:
        # Fecha a conexão DuckDB — as views somem, memória liberada
        con.close()


if __name__ == "__main__":
    run_gold_pipeline()