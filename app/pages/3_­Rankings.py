"""
3_🏆_Rankings.py — usa gold.discografia_summary, que já vem pronta com
rankings calculados no SQL (RANK() OVER). Aqui é só apresentar bem.
"""

import streamlit as st

from utils.db import load_discografia_summary
from utils.style import inject_css

st.set_page_config(page_title="Rankings", page_icon="🏆", layout="wide")
inject_css()
st.title("🏆 Rankings da discografia")

discografia = load_discografia_summary()

ranking_escolhido = st.radio(
    "Ordenar por",
    options=["rank_mais_faixas", "rank_duracao_total", "rank_media_duracao"],
    format_func=lambda r: {
        "rank_mais_faixas": "Mais faixas",
        "rank_duracao_total": "Maior duração total",
        "rank_media_duracao": "Maior duração média por faixa",
    }[r],
    horizontal=True,
)

ordenado = discografia.sort_values(ranking_escolhido)

# ──────────────────────────────────────────────────────────────────────────
# Pódio (top 3) em destaque
# ──────────────────────────────────────────────────────────────────────────
top3 = ordenado.head(3)
medalhas = ["🥇", "🥈", "🥉"]
cols = st.columns(3)
for col, medalha, (_, album) in zip(cols, medalhas, top3.iterrows()):
    with col:
        st.image(album["capa_album_300"], use_container_width=True)
        st.markdown(f"### {medalha} {album['nome_album']}")
        st.write(f"🎵 {int(album['total_musicas'])} faixas · ⏱️ {album['duracao_total_hms']}")

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Tabela completa
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Tabela completa")
st.dataframe(
    ordenado[
        [
            "nome_album",
            "total_musicas",
            "duracao_total_hms",
            "media_duracao_tracks",
            "musica_mais_longa",
            "duracao_mais_longa",
            "musica_mais_curta",
            "duracao_mais_curta",
        ]
    ].rename(
        columns={
            "nome_album": "Álbum",
            "total_musicas": "Faixas",
            "duracao_total_hms": "Duração total",
            "media_duracao_tracks": "Média (min)",
            "musica_mais_longa": "Faixa mais longa",
            "duracao_mais_longa": "Duração (mais longa)",
            "musica_mais_curta": "Faixa mais curta",
            "duracao_mais_curta": "Duração (mais curta)",
        }
    ),
    use_container_width=True,
    hide_index=True,
)
