import streamlit as st
import pandas as pd
import plotly.express as px
from collections import Counter
import re
import os

# ---------------------------------------------------------------------------
# PÁGINA
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Cobertura Mediática – Sonora",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS — selectores verificados contra DOM real
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Inter', 'Source Sans', sans-serif !important;
}
.block-container {
    padding: 1.5rem 2.5rem 3rem 2.5rem !important;
    max-width: 1280px !important;
}

/* ── Header principal ── */
h1 {
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    color: #0f172a !important;
    letter-spacing: -0.02em !important;
    line-height: 1.2 !important;
}
h2, h3 {
    font-weight: 600 !important;
    color: #1e293b !important;
    letter-spacing: -0.01em !important;
}

/* ── Métricas — selector real confirmado ── */
[data-testid="stMetric"] {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-top: 3px solid #C0392B !important;
    border-radius: 10px !important;
    padding: 1.25rem 1.4rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08) !important;
}
[data-testid="stMetricLabel"] > div {
    color: #64748b !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.07em !important;
}
[data-testid="stMetricValue"] > div {
    color: #0f172a !important;
    font-size: 2.1rem !important;
    font-weight: 700 !important;
    line-height: 1.1 !important;
    letter-spacing: -0.02em !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: #0f172a !important;
    border-right: 1px solid #1e293b !important;
}
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] div {
    color: #cbd5e1 !important;
}
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] strong {
    color: #f1f5f9 !important;
}
/* Tags del multiselect */
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background-color: #C0392B !important;
    border: none !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] span {
    color: #ffffff !important;
}
/* Input fields en sidebar */
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] [data-baseweb="input"] {
    background: #1e293b !important;
    border-color: #334155 !important;
    color: #f1f5f9 !important;
}
[data-testid="stSidebar"] hr {
    border-color: #1e293b !important;
}

/* ── Subheaders con línea acento ── */
h3 {
    padding-bottom: 0.35rem !important;
    border-bottom: 2px solid #f1f5f9 !important;
    margin-bottom: 0.75rem !important;
}

/* ── Tabla de artículos ── */
.art-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    font-family: 'Inter', sans-serif;
}
.art-table thead tr {
    background: #0f172a;
}
.art-table th {
    color: #f1f5f9;
    padding: 11px 16px;
    text-align: left;
    font-weight: 600;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.art-table tbody tr:hover { background: #fef2f2; }
.art-table tbody tr:nth-child(even) { background: #f8fafc; }
.art-table td {
    padding: 10px 16px;
    border-bottom: 1px solid #e2e8f0;
    vertical-align: top;
    color: #1e293b;
    line-height: 1.4;
}
.art-table td.titulo-col { max-width: 440px; }
.art-table a { color: #C0392B; text-decoration: none; font-weight: 500; }
.art-table a:hover { text-decoration: underline; }

/* ── Badges de fuente ── */
.badge {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 99px;
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-top: 3px;
}
.bg-gdelt   { background: #dbeafe; color: #1d4ed8; }
.bg-rss     { background: #dcfce7; color: #15803d; }
.bg-youtube { background: #fee2e2; color: #b91c1c; }
.bg-x       { background: #f1f5f9; color: #475569; }

/* ── Botón descarga ── */
[data-testid="stDownloadButton"] > button {
    background: #C0392B !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    padding: 0.55rem 1.4rem !important;
    transition: background 0.15s !important;
}
[data-testid="stDownloadButton"] > button:hover {
    background: #96281b !important;
}

/* ── Dividers ── */
hr { border-color: #e2e8f0 !important; margin: 1.5rem 0 !important; }

/* ── Expander ── */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
}

/* ── Info banner ── */
[data-testid="stAlert"] {
    border-radius: 8px !important;
    font-size: 0.85rem !important;
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# STOPWORDS — lista estática, sin dependencia de NLTK
# ---------------------------------------------------------------------------
STOPWORDS_ES = {
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las",
    "por", "un", "para", "con", "una", "su", "al", "lo", "como", "más",
    "pero", "sus", "le", "ya", "o", "este", "sí", "porque", "esta",
    "entre", "cuando", "muy", "sin", "sobre", "también", "me", "hasta",
    "hay", "donde", "quien", "desde", "todo", "nos", "durante", "todos",
    "uno", "les", "ni", "contra", "otros", "ese", "eso", "ante", "ellos",
    "e", "esto", "mí", "antes", "algunos", "qué", "unos", "yo", "otro",
    "otras", "otra", "él", "tanto", "esa", "estos", "mucho", "quienes",
    "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas",
    "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus",
    "si", "ser", "así", "solo", "tan", "bien", "ya", "dice", "dijo",
    "hay", "van", "ver", "tras", "dos", "tres", "también",
    "puede", "sin", "con", "sus", "son", "fue", "han", "son", "está",
    "son", "ha", "era", "ser", "son", "han", "años", "nuevo", "nueva",
}

# ---------------------------------------------------------------------------
# MAPA DE NOMBRES — GDELT devuelve dominios, los normalizamos
# ---------------------------------------------------------------------------
DOMAIN_MAP = {
    "elimparcial.com":      "El Imparcial Sonora",
    "expreso.com.mx":       "Expreso Hermosillo",
    "tribuna.com.mx":       "Tribuna Sonora",
    "eluniversal.com.mx":   "El Universal",
    "milenio.com":          "Milenio",
    "animalpolitico.com":   "Animal Político",
    "jornada.com.mx":       "La Jornada",
    "proceso.com.mx":       "Proceso",
    "sinembargo.mx":        "Sin Embargo",
    "aristeguinoticias.com":"Aristegui Noticias",
    "reforma.com":          "Reforma",
    "excelsior.com.mx":     "Excélsior",
    "infobae.com":          "Infobae México",
    "debate.com.mx":        "El Debate",
    "ntrguadalajara.net":   "NTR Guadalajara",
}

def normalize_medio(medio: str) -> str:
    key = medio.strip().lower().replace("www.", "")
    return DOMAIN_MAP.get(key, medio)

# ---------------------------------------------------------------------------
# DATASET SINTÉTICO DE RESPALDO
# ---------------------------------------------------------------------------
_SAMPLE_RECORDS = [
    ("Alfonso Durazo presenta plan de seguridad para municipios de Sonora",    "https://elimparcial.com/001", "2024-10-03", "El Imparcial Sonora",  "rss"),
    ("Congreso de Sonora aprueba presupuesto estatal 2025",                    "https://expreso.com.mx/002",  "2024-10-07", "Expreso Hermosillo",   "rss"),
    ("Elecciones municipales en Sonora: candidatos ya registrados",            "https://tribuna.com.mx/003",  "2024-10-11", "Tribuna Sonora",       "gdelt"),
    ("Gobierno de Sonora anuncia inversión en infraestructura vial",           "https://elimparcial.com/004", "2024-10-15", "El Imparcial Sonora",  "rss"),
    ("Operativo contra crimen organizado en Cajeme deja 5 detenidos",          "https://milenio.com/005",     "2024-10-18", "Milenio",              "gdelt"),
    ("Sonora incrementa presupuesto para educación en zonas rurales",          "https://animalpolitico.com/006","2024-10-22","Animal Político",     "rss"),
    ("Alcalde de Hermosillo presenta informe de gobierno anual",               "https://expreso.com.mx/007",  "2024-10-25", "Expreso Hermosillo",   "rss"),
    ("Piden renuncia de funcionarios en municipio de Nogales",                 "https://eluniversal.com/008", "2024-10-29", "El Universal",         "gdelt"),
    ("Alfonso Durazo se reúne con secretario de Gobernación en CDMX",         "https://reforma.com/009",     "2024-11-02", "Reforma",              "gdelt"),
    ("Sequía en Sonora pone en riesgo abasto de agua en Hermosillo",          "https://jornada.com.mx/010",  "2024-11-06", "La Jornada",           "rss"),
    ("Partidos políticos inician precampañas en Sonora",                       "https://proceso.com.mx/011",  "2024-11-09", "Proceso",              "gdelt"),
    ("Gobierno estatal de Sonora refuerza vigilancia en frontera con Sinaloa", "https://elimparcial.com/012", "2024-11-13", "El Imparcial Sonora",  "rss"),
    ("Sindicatos de Sonora marchan frente al Congreso local",                  "https://sinembargo.mx/013",   "2024-11-17", "Sin Embargo",          "rss"),
    ("Madres Buscadoras de Sonora hallan restos en rancho de Hermosillo",      "https://aristegui.mx/014",    "2024-11-20", "Aristegui Noticias",   "gdelt"),
    ("Candidatos a alcaldía de Obregón debaten propuestas de seguridad",      "https://tribuna.com.mx/015",  "2024-11-24", "Tribuna Sonora",       "rss"),
    ("Detienen a exfuncionario de Sonora por desvío de fondos públicos",      "https://reforma.com/016",     "2024-11-28", "Reforma",              "gdelt"),
    ("Protestas en Guaymas contra contaminación industrial en la bahía",       "https://jornada.com.mx/017",  "2024-12-02", "La Jornada",           "rss"),
    ("Alfonso Durazo anuncia bono para policías estatales de Sonora",         "https://elimparcial.com/018", "2024-12-06", "El Imparcial Sonora",  "gdelt"),
    ("Congreso de Sonora aprueba reformas al código penal local",              "https://expreso.com.mx/019",  "2024-12-10", "Expreso Hermosillo",   "rss"),
    ("Balance político de Sonora: los hechos más relevantes del año",          "https://animalpolitico.com/020","2024-12-15","Animal Político",     "rss"),
    ("Nuevo secretario de seguridad toma protesta en Sonora",                  "https://milenio.com/021",     "2024-12-19", "Milenio",              "gdelt"),
    ("Ayuntamiento de Hermosillo aprueba alza en predial para 2025",          "https://expreso.com.mx/022",  "2024-12-23", "Expreso Hermosillo",   "rss"),
    ("Sonora registra incremento en homicidios dolosos según FGJES",          "https://proceso.com.mx/023",  "2024-12-27", "Proceso",              "gdelt"),
    ("Inversión extranjera en Sonora supera 800 millones de dólares",         "https://eluniversal.com/024", "2025-01-05", "El Universal",         "gdelt"),
    ("Alfonso Durazo presenta plan de gobierno para primer trimestre 2025",    "https://elimparcial.com/025", "2025-01-10", "El Imparcial Sonora",  "rss"),
]

def build_sample_df() -> pd.DataFrame:
    df = pd.DataFrame(_SAMPLE_RECORDS, columns=["titulo", "url", "fecha", "medio", "fuente"])
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["contenido"] = ""
    return df.sort_values("fecha", ascending=False).reset_index(drop=True)

# ---------------------------------------------------------------------------
# CARGA DE DATOS
# ---------------------------------------------------------------------------
DATA_FILE = os.path.join(os.path.dirname(__file__), "articulos.csv")

# Detecta títulos claramente en inglés: solo aplica a GDELT/YouTube.
# Criterio: título con ≥3 palabras y ninguna palabra común en español de 4+ letras.
_ES_WORDS = re.compile(
    r"\b(para|como|cuando|pero|desde|hasta|sobre|entre|también|según|"
    r"gobierno|sonora|seguridad|hermosillo|durazo|congreso|municipio|"
    r"alcalde|elecciones|presupuesto|policía|operativo|madres|cajeme|"
    r"nogales|guaymas|reforma|inversión|inversion|años|artículos|"
    r"tras|ante|bajo|está|están|será|será|tiene|tienen|hace|hacen|"
    r"dice|dijo|señaló|informó|anunció|destacó|aseguró)\b",
    re.IGNORECASE,
)

_MEDIOS_LOCALES = {m.lower() for m in [
    "El Imparcial Sonora", "Expreso Hermosillo", "Tribuna Sonora",
    "El Diario de Sonora", "Notisonora",
]}

def _is_english_title(titulo: str, medio: str) -> bool:
    """True si el título parece inglés y el medio NO es local de Sonora."""
    if medio.lower() in _MEDIOS_LOCALES:
        return False   # medios locales siempre en español
    words = titulo.split()
    if len(words) < 4:
        return False   # títulos muy cortos no son clasificables
    return not bool(_ES_WORDS.search(titulo))


@st.cache_data(show_spinner="Cargando datos…")
def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"])
    for col in ("titulo", "medio", "fuente"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    # Normalizar nombres de medios (GDELT devuelve dominios)
    df["medio"] = df["medio"].apply(normalize_medio)

    # Filtrar solo títulos claramente en inglés de fuentes no locales
    if "titulo" in df.columns:
        mask = ~df.apply(
            lambda r: _is_english_title(str(r["titulo"]), str(r.get("medio", ""))),
            axis=1,
        )
        df = df[mask]

    return df.sort_values("fecha", ascending=False).reset_index(drop=True)


def extract_keywords(series: pd.Series, top_n: int = 15) -> pd.DataFrame:
    words = []
    for text in series:
        tokens = re.findall(r"\b[a-záéíóúüñ]{4,}\b", text.lower())
        words.extend([w for w in tokens if w not in STOPWORDS_ES])
    counts = Counter(words).most_common(top_n)
    return pd.DataFrame(counts, columns=["Palabra", "Frecuencia"])

# ── Paleta de gráficas ──
C_RED   = "#C0392B"
C_NAVY  = "#0f172a"
C_SLATE = "#475569"
CHART_FONT = dict(family="Inter, sans-serif", size=12, color="#1e293b")

def _base_layout(**kwargs):
    return dict(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=CHART_FONT,
        margin=dict(l=16, r=16, t=24, b=16),
        **kwargs,
    )

# ---------------------------------------------------------------------------
# INICIALIZACIÓN
# ---------------------------------------------------------------------------
_using_sample = not os.path.exists(DATA_FILE)
df_raw = load_csv(DATA_FILE) if not _using_sample else build_sample_df()

# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        "<div style='padding:1rem 0 0.5rem;'>"
        "<span style='font-size:1.3rem;font-weight:700;color:#f1f5f9;letter-spacing:-0.01em;'>"
        "📰 Monitor Sonora</span><br>"
        "<span style='font-size:0.72rem;color:#64748b;text-transform:uppercase;"
        "letter-spacing:0.08em;'>Cobertura mediática</span>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.divider()

    fecha_min = df_raw["fecha"].min().date()
    fecha_max = df_raw["fecha"].max().date()

    st.markdown("**Período**")
    rango = st.date_input(
        "", value=(fecha_min, fecha_max),
        min_value=fecha_min, max_value=fecha_max,
        label_visibility="collapsed",
    )

    st.markdown("**Medios**")
    medios_disponibles = sorted(df_raw["medio"].unique().tolist())
    medios_sel = st.multiselect(
        "", options=medios_disponibles, default=medios_disponibles,
        placeholder="Todos los medios…", label_visibility="collapsed",
    )

    st.markdown("**Fuente**")
    fuentes_disponibles = sorted(df_raw["fuente"].unique().tolist()) if "fuente" in df_raw.columns else []
    fuentes_sel = st.multiselect(
        "", options=fuentes_disponibles, default=fuentes_disponibles,
        placeholder="gdelt / rss…", label_visibility="collapsed",
    )

    st.divider()
    st.markdown(
        "<p style='font-size:0.7rem;color:#475569;text-align:center;margin:0;'>"
        "Fuentes: GDELT · RSS · YouTube<br>"
        "<span style='color:#334155;'>100 % offline</span></p>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# FILTROS
# ---------------------------------------------------------------------------
if isinstance(rango, (list, tuple)) and len(rango) == 2:
    fecha_inicio, fecha_fin = pd.Timestamp(rango[0]), pd.Timestamp(rango[1])
else:
    fecha_inicio, fecha_fin = pd.Timestamp(fecha_min), pd.Timestamp(fecha_max)

df = df_raw[
    (df_raw["fecha"].dt.normalize() >= fecha_inicio)
    & (df_raw["fecha"].dt.normalize() <= fecha_fin)
]
if medios_sel:
    df = df[df["medio"].isin(medios_sel)]
if fuentes_sel and "fuente" in df.columns:
    df = df[df["fuente"].isin(fuentes_sel)]
df = df.reset_index(drop=True)

# ---------------------------------------------------------------------------
# HEADER
# ---------------------------------------------------------------------------
if _using_sample:
    st.info("**Modo demostración** — Coloca `articulos.csv` en la carpeta del proyecto para cargar datos reales.", icon="ℹ️")

if len(df) == 0:
    st.warning("No hay artículos con los filtros seleccionados.")
    st.stop()

f_min_str = df["fecha"].min().strftime("%d %b %Y")
f_max_str = df["fecha"].max().strftime("%d %b %Y")

st.markdown(
    f"<h1>Análisis de cobertura mediática — Sonora</h1>"
    f"<p style='color:#64748b;font-size:0.9rem;margin-top:-0.5rem;margin-bottom:1.5rem;'>"
    f"Período analizado: <strong style='color:#0f172a;'>{f_min_str}</strong> → "
    f"<strong style='color:#0f172a;'>{f_max_str}</strong> "
    f"&nbsp;·&nbsp; <strong style='color:#C0392B;'>{len(df):,} artículos</strong> encontrados"
    f"</p>",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# MÉTRICAS
# ---------------------------------------------------------------------------
dias_unicos   = df["fecha"].dt.date.nunique()
medios_unicos = df["medio"].nunique()
promedio_dia  = round(len(df) / dias_unicos, 1) if dias_unicos else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total de artículos", f"{len(df):,}")
c2.metric("Medios únicos",      medios_unicos)
c3.metric("Días analizados",    dias_unicos)
c4.metric("Promedio / día",     promedio_dia)

st.divider()

# ---------------------------------------------------------------------------
# TIMELINE
# ---------------------------------------------------------------------------
st.subheader("Actividad diaria")

# Detectar batch-fetch de YouTube: si ≥80% de los artículos de YouTube
# comparten exactamente la misma fecha, sus fechas son de descarga, no
# de publicación — se excluyen de la línea de tiempo para no distorsionarla.
df_timeline = df.copy()
_yt = df_timeline[df_timeline["fuente"].str.lower() == "youtube"]
_yt_batch = False
if len(_yt) >= 3:
    top_fecha_pct = _yt["fecha"].dt.date.value_counts(normalize=True).iloc[0]
    if top_fecha_pct >= 0.8:
        _yt_batch = True
        df_timeline = df_timeline[df_timeline["fuente"].str.lower() != "youtube"]

if _yt_batch:
    st.caption(
        f"ℹ️ Los {len(_yt)} artículos de YouTube se omiten de esta gráfica porque "
        f"su fecha refleja cuándo fueron descargados, no cuándo se publicaron. "
        f"Aparecen en la tabla de artículos y en la sección de fuentes."
    )

timeline = (
    df_timeline.groupby(df_timeline["fecha"].dt.date).size()
    .reset_index(name="Artículos")
    .rename(columns={"fecha": "Fecha"})
)

fig_line = px.area(
    timeline, x="Fecha", y="Artículos",
    template="plotly_white",
    color_discrete_sequence=[C_RED],
)
fig_line.update_traces(
    line=dict(width=2, color=C_RED),
    fillcolor="rgba(192,57,43,0.08)",
    marker=dict(size=4, color=C_RED),
)
fig_line.update_layout(
    **_base_layout(height=280),
    xaxis=dict(showgrid=False, title=None, tickfont=dict(size=11)),
    yaxis=dict(gridcolor="#f1f5f9", title=None, tickfont=dict(size=11)),
)
st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# MEDIOS + PALABRAS CLAVE
# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2, gap="large")

with col_left:
    st.subheader("Medios más activos")
    top_medios = (
        df["medio"].value_counts().head(10)
        .reset_index()
        .rename(columns={"medio": "Medio", "count": "Artículos"})
        .sort_values("Artículos")
    )
    fig_bar = px.bar(
        top_medios, x="Artículos", y="Medio", orientation="h",
        template="plotly_white", color_discrete_sequence=[C_RED],
        text="Artículos",
    )
    fig_bar.update_traces(textposition="outside", textfont=dict(size=11))
    fig_bar.update_layout(
        **_base_layout(height=360),
        xaxis=dict(showgrid=False, title=None, showticklabels=False),
        yaxis=dict(title=None, tickfont=dict(size=12)),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("Palabras clave en titulares")
    kw_df = extract_keywords(df["titulo"], top_n=15)
    if not kw_df.empty:
        fig_kw = px.bar(
            kw_df.sort_values("Frecuencia"),
            x="Frecuencia", y="Palabra", orientation="h",
            template="plotly_white", color_discrete_sequence=[C_NAVY],
            text="Frecuencia",
        )
        fig_kw.update_traces(textposition="outside", textfont=dict(size=11))
        fig_kw.update_layout(
            **_base_layout(height=360),
            xaxis=dict(showgrid=False, title=None, showticklabels=False),
            yaxis=dict(title=None, tickfont=dict(size=12)),
        )
        st.plotly_chart(fig_kw, use_container_width=True)
    else:
        st.info("No hay suficientes títulos para calcular palabras clave.")

st.divider()

# ---------------------------------------------------------------------------
# TABLA DE ARTÍCULOS
# ---------------------------------------------------------------------------
st.subheader("Artículos recientes")

_BADGE = {
    "gdelt":   ("bg-gdelt",   "GDELT"),
    "rss":     ("bg-rss",     "RSS"),
    "youtube": ("bg-youtube", "YouTube"),
    "x":       ("bg-x",       "X"),
}

rows = ""
for _, row in df.iterrows():
    titulo = str(row.get("titulo", ""))
    medio  = str(row.get("medio",  ""))
    fuente = str(row.get("fuente", "")).lower().strip()
    fecha  = row["fecha"].strftime("%d/%m/%Y") if pd.notna(row.get("fecha")) else ""
    url    = str(row.get("url", ""))
    cls, lbl = _BADGE.get(fuente, ("bg-rss", fuente.upper()))
    badge  = f'<span class="badge {cls}">{lbl}</span>'
    enlace = f'<a href="{url}" target="_blank">Ver →</a>' if url else "—"
    rows += (
        f"<tr>"
        f"<td class='titulo-col'>{titulo}</td>"
        f"<td>{medio}<br>{badge}</td>"
        f"<td style='white-space:nowrap;color:#64748b;font-size:0.82rem;'>{fecha}</td>"
        f"<td style='text-align:center;'>{enlace}</td>"
        f"</tr>"
    )

st.markdown(
    f"<table class='art-table'>"
    f"<thead><tr><th>Título</th><th>Medio</th><th>Fecha</th><th>Enlace</th></tr></thead>"
    f"<tbody>{rows}</tbody></table>",
    unsafe_allow_html=True,
)

st.divider()

# ---------------------------------------------------------------------------
# FUENTES — resumen + detalle verificable
# ---------------------------------------------------------------------------
st.subheader("📋 Cobertura por fuente")

fuentes_resumen = (
    df["medio"].value_counts()
    .reset_index()
    .rename(columns={"medio": "Medio", "count": "Artículos"})
)

fig_src = px.bar(
    fuentes_resumen.head(15),
    x="Medio", y="Artículos",
    template="plotly_white",
    color_discrete_sequence=[C_RED],
    text="Artículos",
)
fig_src.update_traces(textposition="outside", textfont=dict(size=11))
fig_src.update_layout(
    **_base_layout(height=360),
    xaxis=dict(tickangle=-35, title=None, tickfont=dict(size=11)),
    yaxis=dict(showgrid=True, gridcolor="#f1f5f9", title=None, showticklabels=False),
)
st.plotly_chart(fig_src, use_container_width=True)

with st.expander("🔍 Ver todas las fuentes verificables", expanded=False):
    st.markdown(
        "<p style='color:#64748b;font-size:0.82rem;margin-bottom:1rem;'>"
        "Lista completa de artículos organizados por medio. "
        "Haz clic en cada enlace para verificar la fuente original.</p>",
        unsafe_allow_html=True,
    )
    for medio, grupo in df.groupby("medio", sort=False):
        st.markdown(f"**{medio}** · {len(grupo)} artículos")
        items = ""
        for _, row in grupo.iterrows():
            titulo = str(row.get("titulo", "Sin título"))
            url    = str(row.get("url", ""))
            fecha  = row["fecha"].strftime("%d/%m/%Y") if pd.notna(row.get("fecha")) else ""
            if url:
                items += f"<li><a href='{url}' target='_blank'>{titulo}</a> <span style='color:#94a3b8;font-size:0.78rem;'>· {fecha}</span></li>"
            else:
                items += f"<li>{titulo} <span style='color:#94a3b8;font-size:0.78rem;'>· {fecha}</span></li>"
        st.markdown(f"<ul style='font-size:0.84rem;line-height:1.7;'>{items}</ul>", unsafe_allow_html=True)
        st.divider()

st.divider()

# ---------------------------------------------------------------------------
# EXPORTAR
# ---------------------------------------------------------------------------
@st.cache_data
def _to_csv(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")

col_dl, col_info = st.columns([1, 3])
with col_dl:
    st.download_button(
        label="⬇ Descargar CSV filtrado",
        data=_to_csv(df),
        file_name="articulos_sonora_filtrado.csv",
        mime="text/csv",
    )
with col_info:
    st.markdown(
        f"<p style='color:#64748b;font-size:0.82rem;padding-top:0.6rem;'>"
        f"{len(df):,} artículos · {medios_unicos} medios · "
        f"{f_min_str} → {f_max_str}</p>",
        unsafe_allow_html=True,
    )
