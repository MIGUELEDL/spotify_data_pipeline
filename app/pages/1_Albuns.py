"""
1_💿_Álbuns.py — galeria de álbuns com filtro por década e detalhe de faixas.

Conceitos de Streamlit usados aqui que ainda não apareceram em app.py:
  - st.session_state: guarda o álbum selecionado entre reruns
  - st.selectbox com format_func: mostra um texto bonito mas guarda o id
  - st.expander: esconde conteúdo até o usuário clicar
"""

import pandas as pd
import plotly.express as px
import streamlit as st

from utils.db import load_albums_enriched, load_tracks_enriched
from utils.style import PALETTE, PLOTLY_LAYOUT, decada_sort_key, inject_css

st.set_page_config(page_title="Álbuns", page_icon="💿", layout="wide")
inject_css()
st.title("💿 Álbuns")

albums = load_albums_enriched()
tracks = load_tracks_enriched()
albums["data_lancamento"] = pd.to_datetime(albums["data_lancamento"])

# ──────────────────────────────────────────────────────────────────────────
# Filtro por década na sidebar — reaproveitado por toda a página
# ──────────────────────────────────────────────────────────────────────────
decadas = sorted(albums["decada"].dropna().unique(), key=decada_sort_key)
decadas_selecionadas = st.sidebar.multiselect(
    "Década", options=decadas, default=decadas
)

albums_filtrados = albums[albums["decada"].isin(decadas_selecionadas)].sort_values(
    "data_lancamento"
)

if albums_filtrados.empty:
    st.info("Nenhum álbum para as décadas selecionadas.")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────
# Galeria de capas — st.columns em loop é o jeito mais simples de fazer
# um "grid" no Streamlit (não existe um componente de grid nativo)
# ──────────────────────────────────────────────────────────────────────────
st.subheader(f"{len(albums_filtrados)} álbum(ns)")
N_COLS = 5
linhas = [albums_filtrados.iloc[i : i + N_COLS] for i in range(0, len(albums_filtrados), N_COLS)]

for linha in linhas:
    cols = st.columns(N_COLS)
    for col, (_, album) in zip(cols, linha.iterrows()):
        with col:
            st.image(album["capa_album_300"], use_container_width=True)
            st.caption(f"**{album['nome_album']}**  \n{album['data_lancamento'].year} · {int(album['qtd_faixas'])} faixas")

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Duração total por álbum (gráfico de barras horizontal)
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Duração total por álbum")
fig = px.bar(
    albums_filtrados.sort_values("duracao_total_min"),
    x="duracao_total_min",
    y="nome_album",
    orientation="h",
    color="decada",
    color_discrete_sequence=PALETTE,
    labels={"duracao_total_min": "Minutos", "nome_album": "Álbum", "decada": "Década"},
)
fig.update_layout(**PLOTLY_LAYOUT, height=max(350, 28 * len(albums_filtrados)))
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Detalhe: escolher um álbum e ver as faixas dele
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Faixas de um álbum")

album_escolhido_id = st.selectbox(
    "Escolha um álbum",
    options=albums_filtrados["id_album"],
    format_func=lambda id_: albums_filtrados.loc[
        albums_filtrados["id_album"] == id_, "nome_album"
    ].iloc[0],
)

album_info = albums_filtrados[albums_filtrados["id_album"] == album_escolhido_id].iloc[0]
faixas_do_album = tracks[tracks["id_album"] == album_escolhido_id].sort_values("numero_no_album")

col_capa, col_tabela = st.columns([1, 3])
with col_capa:
    st.image(album_info["capa_album_300"], use_container_width=True)
    st.metric("Faixas", int(album_info["qtd_faixas"]))
    st.metric("Duração total", album_info["duracao_total_hms"])
    st.metric("Duração média por faixa", f"{album_info['media_duracao_tracks']:.1f} min")

with col_tabela:
    st.dataframe(
        faixas_do_album[
            [
                "numero_no_album",
                "nome_musica",
                "duracao_min_seg",
                "posicao_no_album",
                "faixas_classificacao",
                "conteudo_explicito",
            ]
        ].rename(
            columns={
                "numero_no_album": "#",
                "nome_musica": "Faixa",
                "duracao_min_seg": "Duração",
                "posicao_no_album": "Posição",
                "faixas_classificacao": "Classificação",
                "conteudo_explicito": "Conteúdo explícito",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
