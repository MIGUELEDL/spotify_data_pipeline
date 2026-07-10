"""
2_🎵_Faixas.py — busca e filtro em todas as faixas da discografia.

Conceitos novos:
  - st.text_input pra busca livre (com .str.contains)
  - st.tabs pra organizar conteúdo sem gastar espaço vertical
  - st.column_config pra deixar o st.dataframe mais rico (link clicável)
"""

import plotly.express as px
import streamlit as st

from utils.db import load_tracks_enriched
from utils.style import PALETTE, PLOTLY_LAYOUT, inject_css

st.set_page_config(page_title="Faixas", page_icon="🎵", layout="wide")
inject_css()
st.title("🎵 Faixas")

tracks = load_tracks_enriched()

# ──────────────────────────────────────────────────────────────────────────
# Filtros na sidebar
# ──────────────────────────────────────────────────────────────────────────
busca = st.sidebar.text_input("Buscar faixa pelo nome")

albuns_disponiveis = sorted(tracks["nome_album"].unique())
albuns_selecionados = st.sidebar.multiselect(
    "Álbum", options=albuns_disponiveis, default=albuns_disponiveis
)

classificacoes = sorted(tracks["faixas_classificacao"].unique())
classificacoes_selecionadas = st.sidebar.multiselect(
    "Duração", options=classificacoes, default=classificacoes
)

apenas_explicitas = st.sidebar.checkbox("Mostrar só com conteúdo explícito")

filtrado = tracks[
    tracks["nome_album"].isin(albuns_selecionados)
    & tracks["faixas_classificacao"].isin(classificacoes_selecionadas)
]
if busca:
    filtrado = filtrado[filtrado["nome_musica"].str.contains(busca, case=False, na=False)]
if apenas_explicitas:
    filtrado = filtrado[filtrado["conteudo_explicito"] == "contém"]

st.caption(f"{len(filtrado)} de {len(tracks)} faixas")

# ──────────────────────────────────────────────────────────────────────────
# Abas: Tabela / Distribuição / Rankings
# ──────────────────────────────────────────────────────────────────────────
aba_tabela, aba_distribuicao, aba_ranking = st.tabs(
    ["📋 Tabela", "📊 Distribuição de duração", "🏆 Mais longas / mais curtas"]
)

with aba_tabela:
    st.dataframe(
        filtrado[
            [
                "nome_musica",
                "nome_album",
                "duracao_min_seg",
                "faixas_classificacao",
                "posicao_no_album",
                "conteudo_explicito",
                "url_da_musica",
            ]
        ].rename(
            columns={
                "nome_musica": "Faixa",
                "nome_album": "Álbum",
                "duracao_min_seg": "Duração",
                "faixas_classificacao": "Classificação",
                "posicao_no_album": "Posição no álbum",
                "conteudo_explicito": "Conteúdo explícito",
                "url_da_musica": "Spotify",
            }
        ),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Spotify": st.column_config.LinkColumn("Spotify", display_text="Abrir ↗")
        },
    )

with aba_distribuicao:
    fig = px.histogram(
        filtrado,
        x="faixas_classificacao",
        color="faixas_classificacao",
        color_discrete_sequence=PALETTE,
        category_orders={
            "faixas_classificacao": [
                "curta (< 1:30)",
                "média (1:30 - 3:00)",
                "padrão (3:00 - 5:00)",
                "longa (5:00 - 8:00)",
                "muito longa (> 8:00)",
            ]
        },
        labels={"faixas_classificacao": "Classificação"},
    )
    fig.update_layout(**PLOTLY_LAYOUT, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with aba_ranking:
    col_longas, col_curtas = st.columns(2)
    with col_longas:
        st.markdown("**Top 10 faixas mais longas**")
        st.dataframe(
            filtrado.nsmallest(10, "rank_duracao_global")[
                ["nome_musica", "nome_album", "duracao_min_seg"]
            ].rename(columns={"nome_musica": "Faixa", "nome_album": "Álbum", "duracao_min_seg": "Duração"}),
            use_container_width=True,
            hide_index=True,
        )
    with col_curtas:
        st.markdown("**Top 10 faixas mais curtas**")
        st.dataframe(
            filtrado.nlargest(10, "rank_duracao_global")[
                ["nome_musica", "nome_album", "duracao_min_seg"]
            ].rename(columns={"nome_musica": "Faixa", "nome_album": "Álbum", "duracao_min_seg": "Duração"}),
            use_container_width=True,
            hide_index=True,
        )
