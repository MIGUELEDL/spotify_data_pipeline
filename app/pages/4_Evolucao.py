"""
4_📈_Evolução.py — responde: a duração das músicas mudou com o tempo?
qual ano/década foi mais produtivo? Usa gold.evolucao_por_decada.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.db import load_evolucao_por_decada
from utils.style import PALETTE, PLOTLY_LAYOUT, decada_sort_key, inject_css

st.set_page_config(page_title="Evolução", page_icon="📈", layout="wide")
inject_css()
st.title("📈 Evolução ao longo do tempo")

evolucao = load_evolucao_por_decada().sort_values("ano")

# Ano mais produtivo
ano_mais_produtivo = evolucao.loc[evolucao["total_musicas"].idxmax()]
c1, c2, c3 = st.columns(3)
c1.metric("Ano mais produtivo", int(ano_mais_produtivo["ano"]), f"{int(ano_mais_produtivo['total_musicas'])} faixas")
c2.metric("Década com mais álbuns", evolucao.groupby("decada")["total_albums"].sum().idxmax())
c3.metric("Duração média histórica", f"{evolucao['media_duracao_min'].mean():.1f} min")

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Duração média das faixas por ano — mostra se as músicas ficaram mais
# longas ou mais curtas com o tempo (com faixa de min/max)
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Duração das faixas ao longo dos anos")
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=evolucao["ano"], y=evolucao["maior_duracao_min"],
    name="Mais longa do ano", line=dict(color=PALETTE[5], width=1, dash="dot"),
))
fig.add_trace(go.Scatter(
    x=evolucao["ano"], y=evolucao["menor_duracao_min"],
    name="Mais curta do ano", line=dict(color=PALETTE[4], width=1, dash="dot"),
    fill="tonexty",
))
fig.add_trace(go.Scatter(
    x=evolucao["ano"], y=evolucao["media_duracao_min"],
    name="Média do ano", line=dict(color=PALETTE[0], width=3),
))
fig.update_layout(**PLOTLY_LAYOUT, yaxis_title="Minutos", xaxis_title="Ano")
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ──────────────────────────────────────────────────────────────────────────
# Comparação entre décadas
# ──────────────────────────────────────────────────────────────────────────
st.subheader("Comparação entre décadas")
por_decada = (
    evolucao.groupby("decada", as_index=False)
    .agg(
        total_albums=("total_albums", "sum"),
        total_musicas=("total_musicas", "sum"),
        total_minutos_gravados=("total_minutos_gravados", "sum"),
        media_duracao_min=("media_duracao_min", "mean"),
    )
)
por_decada = por_decada.sort_values("decada", key=lambda s: s.map(decada_sort_key))

col_a, col_b = st.columns(2)
with col_a:
    fig_a = px.bar(
        por_decada, x="decada", y="total_musicas",
        color="decada", color_discrete_sequence=PALETTE,
        labels={"decada": "Década", "total_musicas": "Faixas gravadas"},
    )
    fig_a.update_layout(**PLOTLY_LAYOUT, showlegend=False)
    st.plotly_chart(fig_a, use_container_width=True)

with col_b:
    fig_b = px.bar(
        por_decada, x="decada", y="media_duracao_min",
        color="decada", color_discrete_sequence=PALETTE,
        labels={"decada": "Década", "media_duracao_min": "Duração média (min)"},
    )
    fig_b.update_layout(**PLOTLY_LAYOUT, showlegend=False)
    st.plotly_chart(fig_b, use_container_width=True)

st.dataframe(
    por_decada.rename(
        columns={
            "decada": "Década",
            "total_albums": "Álbuns",
            "total_musicas": "Faixas",
            "total_minutos_gravados": "Minutos gravados",
            "media_duracao_min": "Duração média (min)",
        }
    ),
    use_container_width=True,
    hide_index=True,
)
