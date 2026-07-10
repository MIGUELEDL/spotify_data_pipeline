"""
app.py — página inicial do dashboard (Visão Geral da discografia).

Como funciona o multipage do Streamlit
---------------------------------------
Este arquivo é o "entrypoint" (é ele que o docker-compose manda rodar).
Qualquer arquivo dentro de app/pages/ vira automaticamente uma página extra
no menu lateral — sem precisar registrar em lugar nenhum. O prefixo numérico
no nome do arquivo (ex: "1_💿_Álbuns.py") só controla a ORDEM no menu; o
emoji vira o ícone e o resto do nome vira o texto do menu.
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.db import (
    load_albums_enriched,
    load_discografia_summary,
    load_evolucao_por_decada,
    load_tracks_enriched,
)
from utils.style import PALETTE, PLOTLY_LAYOUT, fmt_horas, inject_css

# set_page_config precisa ser o primeiro comando Streamlit do script,
# e cada página do app (inclusive as em pages/) chama o seu próprio.
st.set_page_config(
    page_title="Spotify Data Pipeline",
    page_icon="🎧",
    layout="wide",
)
inject_css()

#___________________________________________________________________________________

# Carrega os dados (cacheados — veja utils/db.py)
albums = load_albums_enriched()
tracks = load_tracks_enriched()
discografia = load_discografia_summary()
evolucao = load_evolucao_por_decada()

if albums.empty:
    st.warning(
        "Nenhum dado encontrado em `gold.albums_enriched`. "
        "Rode a DAG `spotify_data_pipeline` no Airflow (localhost:8080) "
        "pelo menos uma vez antes de usar o dashboard."
    )
    st.stop()

albums["data_lancamento"] = pd.to_datetime(albums["data_lancamento"])

#___________________________________________________________________________________

# Cabeçalho
artista_nome = st.sidebar.text_input(
    "Nome do artista (só exibição)", value="Minha Discografia"
)
st.title(f"🎧 {artista_nome}")
st.caption(
    "Dashboard construído sobre a camada **gold** do "
    "[spotify_data_pipeline](https://github.com/MIGUELEDL/spotify_data_pipeline)."
)

#___________________________________________________________________________________

# KPIs — st.columns divide o espaço horizontalmente, st.metric mostra
# um número grande em destaque (ótimo pra dashboards)
total_albuns = len(albums)
total_faixas = int(albums["qtd_faixas"].sum())
total_minutos = albums["duracao_total_min"].sum()
primeiro_lancamento = albums["data_lancamento"].min()
ultimo_lancamento = albums["data_lancamento"].max()
anos_de_carreira = ultimo_lancamento.year - primeiro_lancamento.year + 1

c1, c2, c3, c4 = st.columns(4)
c1.metric("Álbuns", total_albuns)
c2.metric("Faixas", total_faixas)
c3.metric("Tempo total gravado", fmt_horas(total_minutos))
c4.metric("Anos de carreira", anos_de_carreira)

st.divider()

#___________________________________________________________________________________

# Linha do tempo de lançamentos
col_esq, col_dir = st.columns([2, 1])

with col_esq:
    st.subheader("Linha do tempo de lançamentos")
    fig = px.bar(
        albums.sort_values("data_lancamento"),
        x="data_lancamento",
        y="qtd_faixas",
        color="decada",
        color_discrete_sequence=PALETTE,
        hover_name="nome_album",
        labels={
            "data_lancamento": "Lançamento",
            "qtd_faixas": "Nº de faixas",
            "decada": "Década",
        },
    )
    fig.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

with col_dir:
    st.subheader("Faixas por década")
    por_decada = (
        albums.groupby("decada", as_index=False)["qtd_faixas"]
        .sum()
        .sort_values("decada")
    )
    fig2 = px.pie(
        por_decada,
        names="decada",
        values="qtd_faixas",
        color_discrete_sequence=PALETTE,
        hole=0.5,
    )
    fig2.update_layout(**PLOTLY_LAYOUT, showlegend=True)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

#___________________________________________________________________________________

# Destaques rápidos (usando gold.discografia_summary, que já vem com rankings)
st.subheader("Destaques")
d1, d2, d3 = st.columns(3)

mais_faixas = discografia.sort_values("rank_mais_faixas").iloc[0]
mais_longo = discografia.sort_values("rank_duracao_total").iloc[0]
faixa_mais_longa = tracks.sort_values("rank_duracao_global").iloc[0]

with d1:
    st.image(mais_faixas["capa_album_300"], use_container_width=True)
    st.markdown(f"**Álbum com mais faixas**  \n{mais_faixas['nome_album']} ({int(mais_faixas['total_musicas'])} faixas)")

with d2:
    st.image(mais_longo["capa_album_300"], use_container_width=True)
    st.markdown(f"**Álbum mais longo**  \n{mais_longo['nome_album']} ({mais_longo['duracao_total_hms']})")

with d3:
    st.image(faixa_mais_longa["capa_album_300"], use_container_width=True)
    st.markdown(f"**Faixa mais longa**  \n{faixa_mais_longa['nome_musica']} ({faixa_mais_longa['duracao_min_seg']})")

st.divider()
st.caption(
    "Use o menu à esquerda para explorar Álbuns, Faixas, Rankings da "
    "discografia e a Evolução por década em mais detalhe."
)
