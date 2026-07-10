"""
style.py — pequenas funções de formatação + identidade visual do app.

Mantemos isso separado de db.py pra cada arquivo ter uma responsabilidade só
(uma acessa dados, o outro só formata/estiliza). Fica mais fácil de achar
as coisas conforme o app cresce.
"""

import streamlit as st

# Paleta baseada no verde do Spotify, usada nos gráficos Plotly pra manter
# uma identidade visual consistente entre as páginas.
SPOTIFY_GREEN = "#1DB954"
SPOTIFY_BLACK = "#191414"
SPOTIFY_GREY = "#B3B3B3"

# Sequência de cores pra gráficos com várias categorias (décadas, álbuns...)
PALETTE = ["#1DB954", "#1ED760", "#159C46", "#0D5C29", "#535353", "#B3B3B3"]

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#FFFFFF"),
    margin=dict(l=10, r=10, t=40, b=10),
)


def inject_css() -> None:
    """CSS global leve: cards com cantos arredondados e destaque em verde."""
    st.markdown(
        f"""
        <style>
        div[data-testid="stMetric"] {{
            background-color: rgba(29, 185, 84, 0.08);
            border: 1px solid rgba(29, 185, 84, 0.25);
            border-radius: 12px;
            padding: 12px 16px;
        }}
        div[data-testid="stMetricValue"] {{
            color: {SPOTIFY_GREEN};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def fmt_min(minutos: float) -> str:
    """Formata minutos (float) como '3.5 min' -> '3 min 30s'."""
    if minutos is None:
        return "-"
    total_seg = int(round(float(minutos) * 60))
    m, s = divmod(total_seg, 60)
    return f"{m}min {s:02d}s"


def fmt_horas(minutos_totais: float) -> str:
    """Formata minutos totais como '12h 30min', usado em somatórios grandes."""
    if minutos_totais is None:
        return "-"
    total_min = int(round(float(minutos_totais)))
    h, m = divmod(total_min, 60)
    return f"{h}h {m:02d}min" if h else f"{m}min"


def decada_sort_key(decada: str):
    """Transforma '1990s' -> 1990 pra permitir ordenação natural das décadas."""
    try:
        return int(str(decada).replace("s", ""))
    except (TypeError, ValueError):
        return 0
