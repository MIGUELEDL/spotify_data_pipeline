"""
db.py — camada de acesso a dados do app.

Aqui a gente reaproveita o PostgresClient que já existe em `utils/postgres_client.py`
(a raiz do projeto) e só adiciona uma coisa que ele não tem: CACHE.

Por que cache importa no Streamlit?
------------------------------------
Todo clique / interação (mudar um filtro, selecionar uma aba, redimensionar
a tela) faz o Streamlit reexecutar o script INTEIRO de cima a baixo. Sem
cache, isso significa uma nova query no Postgres a cada clique — lento e
desnecessário, já que os dados da camada gold só mudam 1x por dia (quando
a DAG do Airflow roda).

`@st.cache_data` resolve isso: guarda o resultado da função em memória e só
executa a função de novo se (a) os argumentos mudarem ou (b) o `ttl` expirar.
"""

import os
import sys

import pandas as pd
import streamlit as st

# Garante manualmente que a pasta "app/" (onde mora shared/, o link pra
# ./utils da raiz do projeto) está no sys.path.
#
# Por quê isso é necessário mesmo com PYTHONPATH configurado no
# docker-compose? Porque o Streamlit, ao rodar as páginas dentro de
# app/pages/, nem sempre herda o sys.path do jeito que a gente espera —
# então em vez de depender disso, calculamos o caminho a partir da
# localização deste próprio arquivo (__file__) e inserimos manualmente.
# __file__ = /app/app/utils/db.py -> sobe 2 níveis -> /app/app
_APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from shared.postgres_client import PostgresClient  # noqa: E402 (import depois do sys.path.insert de propósito)

# TTL = tempo (em segundos) que o cache fica válido antes de buscar de novo.
# 10 min é um bom equilíbrio: dados atualizam 1x/dia, mas se você rodar a
# DAG manualmente enquanto testa o app, não fica esperando muito tempo.
CACHE_TTL = 600


@st.cache_resource
def get_client() -> PostgresClient:
    """
    Cria UMA instância do PostgresClient e reaproveita entre reruns.

    @st.cache_resource é o irmão do @st.cache_data, mas pra objetos que não
    são "dados" e sim recursos com estado (conexões, clientes, modelos de ML).
    """
    return PostgresClient()


@st.cache_data(ttl=CACHE_TTL, show_spinner="Carregando dados do Postgres...")
def get_table(table: str, schema: str = "gold"):
    """Busca uma tabela inteira do schema gold. Ex: get_table('albums_enriched')."""
    return get_client().read_table(table, schema=schema)


@st.cache_data(ttl=CACHE_TTL, show_spinner="Carregando dados do Postgres...")
def get_query(sql: str):
    """Roda uma query customizada quando uma tabela inteira não é suficiente."""
    return get_client().read_sql(sql)


def _to_numeric(df, colunas):
    """
    Força colunas específicas a serem numéricas, mesmo que tenham vindo
    como TEXT do Postgres (ex: se algum dia o bug de tipagem do
    postgres_client.py voltar). errors='coerce' transforma o que não
    conseguir converter em NaN em vez de quebrar o app inteiro.
    """
    for col in colunas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# Atalhos pra cada uma das 4 tabelas gold do projeto.
# Ter uma função nomeada por tabela deixa as pages mais legíveis do que
# espalhar get_table("albums_enriched") por todo lado.

def load_albums_enriched():
    df = get_table("albums_enriched")
    return _to_numeric(df, ["qtd_faixas", "duracao_total_ms", "duracao_total_min", "media_duracao_tracks"])


def load_tracks_enriched():
    df = get_table("tracks_enriched")
    return _to_numeric(df, ["numero_no_album", "rank_duracao_no_album", "rank_duracao_global"])


def load_discografia_summary():
    df = get_table("discografia_summary")
    return _to_numeric(df, [
        "total_musicas", "duracao_total_min", "media_duracao_tracks",
        "rank_mais_faixas", "rank_duracao_total", "rank_media_duracao",
    ])


def load_evolucao_por_decada():
    df = get_table("evolucao_por_decada")
    return _to_numeric(df, [
        "ano", "total_albums", "total_musicas", "media_duracao_min",
        "menor_duracao_min", "maior_duracao_min", "total_minutos_gravados",
    ])