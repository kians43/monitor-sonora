"""
curate_data.py
Curador estricto: solo conserva artículos con relevancia política directa
sobre el gobierno del estado de Sonora.

Lógica:
  1. Blacklist de título → elimina (entretenimiento, deportes, históricos)
  2. Blacklist de dominio → elimina (medios extranjeros irrelevantes)
  3. Filtro positivo OBLIGATORIO → el título debe contener al menos
     una keyword del conjunto de términos políticos relevantes.
     Sin excepción por fuente.

Uso:
    python curate_data.py

Lee:    articulos.csv
Guarda: articulos.csv  (reemplaza con versión curada)
"""

import pandas as pd
import re
import os
from datetime import datetime

INPUT_FILE  = "articulos.csv"
OUTPUT_FILE = "articulos.csv"

# Período del contrato
FECHA_INICIO = datetime(2025, 12, 1)
FECHA_FIN    = datetime(2026, 3, 9)

# ---------------------------------------------------------------------------
# MEDIOS LOCALES DE SONORA — ya son específicos por definición.
# Para estos, el filtro positivo se relaja: solo se aplica la blacklist.
# Para medios nacionales/GDELT el filtro positivo sigue siendo obligatorio.
# ---------------------------------------------------------------------------
MEDIOS_LOCALES_SONORA = {
    "el imparcial sonora",
    "expreso hermosillo",
    "tribuna sonora",
    "el diario de sonora",
    "notisonora",
    "el regional de sonora",
}

# ---------------------------------------------------------------------------
# 1. BLACKLIST DE TÍTULO — se elimina si el título contiene alguno de estos
# ---------------------------------------------------------------------------
BLACKLIST_TITULO = [
    # El Negro Durazo (Arturo Durazo Morales, jefe policiaco 1970s — distinto
    # de Alfonso Durazo, gobernador actual de Sonora)
    r"negro\s+durazo",
    r"arturo\s+durazo",
    r"el\s+negro\s+durazo",
    r"durazo\s+morales",
    r"partenón.*durazo",
    r"durazo.*partenón",
    r"jefe\s+de\s+la\s+polic[ií]a.*portillo",  # López Portillo era
    r"ventaneando",
    r"luis\s+miguel.*durazo",
    r"luis\s+rey.*durazo",
    r"hijos\s+de.*durazo",

    # Música y entretenimiento
    r"sonora\s+dinamita",
    r"la\s+sonora\s+santanera",
    r"\bsonora\s+de\b",
    r"\bcumbia\b",
    r"banda\s+sonora",        # soundtrack
    r"\bsoundtrack\b",
    r"\bmusical\b",
    r"\bconcierto\b",
    r"\bálbum\b",
    r"\bcanción\b",
    r"\bdisco\b(?!\s+de\s+datos)",  # disco de datos no aplica
    r"grupo\s+musical",
    r"\bnorteño\b",
    r"\bcorrido\b",
    r"\bregaetón\b",
    r"\bregguetón\b",

    # Deportes
    r"\bfútbol\b",
    r"\bfutbol\b",
    r"\bliga\s+mx\b",
    r"\btorneo\b",
    r"partido\s+de\s+fútbol",
    r"\bbeisbol\b",
    r"\bbasquetbol\b",

    # Farándula / entretenimiento general
    r"\bactor\b",
    r"\bactriz\b",
    r"\btelenovela\b",
    r"\bserie\s+de\s+tv\b",
    r"\bnetflix\b",

    # Turismo / gastronomía descontextualizada
    r"receta",
    r"gastronom[ií]a",
    r"playa.*sonora",
    r"turismo.*sonora",

    # Documentales históricos sin relación con gobierno actual
    r"corrupt.*power.*documentary",
    r"documentary.*corrupt",
]

# ---------------------------------------------------------------------------
# 2. BLACKLIST DE DOMINIOS — medios extranjeros / irrelevantes
# ---------------------------------------------------------------------------
BLACKLIST_DOMINIOS = [
    "banjarmasin", "kompas", "tribunnews", "detik.com",
    "liputan6", "okezone", "tempo.co", "sindonews",
    "philstar", "inquirer.net", "rappler.com",
    "valeursactuelles", "lefigaro", "lemonde",
    "kwtx.com", "tucson.com", "azcentral",
    "finance.yahoo.com",
    "dailymail", "theguardian", "reuters.com",
    "bbc.co.uk", "bbc.com",
]

# ---------------------------------------------------------------------------
# 3. FILTRO POSITIVO OBLIGATORIO
#    El título DEBE contener al menos una de estas expresiones.
#    Cubre los temas de la cotización: gobierno estatal, seguridad,
#    política, presupuesto, elecciones, derechos humanos, economía.
# ---------------------------------------------------------------------------
REQUIRED_KEYWORDS = [
    # Gobierno / funcionarios
    r"alfonso\s+durazo",
    r"durazo\s+montaño",
    r"gobernador\s+de\s+sonora",
    r"gobierno\s+de\s+sonora",
    r"gobierno\s+estatal",
    r"secretar[ií]a.*sonora",
    r"congreso\s+de\s+sonora",
    r"congreso\s+local",
    r"diputados.*sonora",
    r"senador.*sonora",
    r"alcalde.*sonora",
    r"alcalde.*hermosillo",
    r"alcalde.*cajeme",
    r"alcalde.*nogales",
    r"alcalde.*guaymas",
    r"ayuntamiento.*sonora",
    r"ayuntamiento.*hermosillo",
    r"cabildo.*sonora",
    r"funcionarios.*sonora",

    # Geografía política obligatoria (ciudad + tema de gobierno)
    r"hermosillo.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",
    r"cajeme.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",
    r"nogales.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",
    r"guaymas.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",
    r"obregón.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",
    r"navojoa.*(presupuesto|seguridad|pol[ií]tic|gobierno|obra|denuncia|protest|elec)",

    # Temas políticos con Sonora explícito
    r"sonora.*(elec|candidat|partido|diputad|senad|presupuesto|invers|seguridad|corrupci|denuncia|protest|march|sindicat|reform|derechos|violen|homicid|desaparec|narcot|crimen|cartel)",
    r"(elec|candidat|partido|presupuesto|invers|seguridad|corrupci|denuncia|protest|reform|derechos).*sonora",

    # Presupuesto / economía política estatal
    r"presupuesto.*sonora",
    r"sonora.*presupuesto",
    r"inversi[oó]n.*sonora",
    r"obra\s+p[úu]blica.*sonora",

    # Seguridad pública / violencia
    r"(operativo|cartel|narco|homicid|feminicid|desaparec|secuestr|extors|violenc).*sonora",
    r"sonora.*(operativo|cartel|narco|homicid|feminicid|desaparec|secuestr|extors|violenc)",
    r"guardia\s+nacional.*sonora",
    r"polic[ií]a.*sonora",
    r"madres\s+buscadoras",
    r"fgjes",

    # Partidos políticos
    r"(pan|pri|morena|prd|mc|pvem|pt\b).*sonora",
    r"sonora.*(pan|pri|morena|prd|mc\b|pvem|pt\b)",
]

# ---------------------------------------------------------------------------
# Compilar patrones
# ---------------------------------------------------------------------------
def _compile(patterns):
    return re.compile("|".join(patterns), re.IGNORECASE | re.DOTALL)

BLACKLIST_TITULO_RE  = _compile(BLACKLIST_TITULO)
BLACKLIST_DOMINIO_RE = _compile(BLACKLIST_DOMINIOS)
REQUIRED_RE          = _compile(REQUIRED_KEYWORDS)


def is_relevant(row) -> tuple[bool, str]:
    titulo = str(row.get("titulo", "")).strip()
    medio  = str(row.get("medio",  "")).strip()
    url    = str(row.get("url",    "")).strip()

    # 1. Blacklist de título (aplica a todos)
    if BLACKLIST_TITULO_RE.search(titulo):
        return False, "blacklist_titulo"

    # 2. Blacklist de dominio (aplica a todos)
    if BLACKLIST_DOMINIO_RE.search(url) or BLACKLIST_DOMINIO_RE.search(medio):
        return False, f"dominio_irrelevante: {medio}"

    # 3. Filtro positivo — aplica a TODOS los medios sin excepción.
    #    Todos los artículos deben contener al menos una keyword política explícita.
    if not REQUIRED_RE.search(titulo):
        return False, "sin_keyword_relevante"

    return True, "ok"


def main():
    print("=" * 50)
    print("curate_data.py — Curador estricto")
    print("=" * 50)

    if not os.path.exists(INPUT_FILE):
        print(f"[!] No se encontró {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE, encoding="utf-8-sig")
    total_original = len(df)
    print(f"  Registros antes : {total_original}")

    # Filtro de fechas — solo el período del contrato
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    antes_fecha = len(df)
    df = df[(df["fecha"] >= FECHA_INICIO) & (df["fecha"] <= FECHA_FIN)]
    fuera_rango = antes_fecha - len(df)
    if fuera_rango:
        print(f"  Fuera de rango  : {fuera_rango} ({FECHA_INICIO.date()} – {FECHA_FIN.date()})")
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)

    resultados = df.apply(is_relevant, axis=1)
    mascaras   = [r[0] for r in resultados]
    razones    = [r[1] for r in resultados]

    eliminados  = df[~pd.Series(mascaras)].copy()
    df_curado   = df[pd.Series(mascaras)].reset_index(drop=True)

    # Resumen de razones de eliminación
    from collections import Counter
    razon_counts = Counter(
        r for r, keep in zip(razones, mascaras) if not keep
    )

    print(f"  Eliminados      : {len(eliminados)}")
    for razon, cnt in razon_counts.most_common():
        print(f"    [{cnt:3d}] {razon}")

    print(f"\n  Registros finales: {len(df_curado)}")
    if "fuente" in df_curado.columns:
        print(f"  Fuentes : {df_curado['fuente'].value_counts().to_dict()}")
    if "medio" in df_curado.columns:
        print(f"  Medios únicos: {df_curado['medio'].nunique()}")

    df_curado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Guardado: {OUTPUT_FILE}")
    print("=" * 50)


if __name__ == "__main__":
    main()
