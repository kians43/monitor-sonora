"""
fetch_data.py
Recolecta artículos sobre monitoreo político en Sonora desde:
  1. GDELT Doc 2.0 API
  2. Feeds RSS (lista RSS_SOURCES con smoke-test integrado)

Uso normal:
    python fetch_data.py

Solo smoke-test (sin ingesta):
    python fetch_data.py --test

Genera: articulos.csv
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
import requests
import feedparser
import pandas as pd
import time
import re
from datetime import datetime, timedelta
from urllib.parse import quote

# ---------------------------------------------------------------------------
# CONFIGURACIÓN GDELT
# ---------------------------------------------------------------------------
KEYWORDS_GDELT = [
    "Sonora",
    "Alfonso Durazo",
    "Gobierno de Sonora",
]
MAX_GDELT_POR_QUERY = 100  # GDELT permite hasta 250 pero timeoutea en queries amplias

# Período exacto de la cotización (Hanakua / Antonio Santiago)
FECHA_INICIO = datetime(2025, 12, 1)
FECHA_FIN    = datetime(2026, 3, 9)

HEADERS = {"User-Agent": "Mozilla/5.0 (research-project/1.0)"}

# ---------------------------------------------------------------------------
# RSS_SOURCES — fuente única de verdad para todos los feeds RSS
#
# Campos:
#   name     : nombre legible del medio (usado como columna "medio" en el CSV)
#   url      : URL del feed RSS
#   scope    : "sonora" | "nacional"  (para logs; el filtro real es SONORA_RE)
#   enabled  : False = no se usa en ingesta ni smoke-test automático
#   note     : razón por la que está desactivado (opcional)
# ---------------------------------------------------------------------------
RSS_SOURCES = [

    # ── Sonora ──────────────────────────────────────────────────────────────
    {
        "name": "El Imparcial Sonora",
        "url": "https://www.elimparcial.com/rss/feed.xml",
        "scope": "sonora",
        "enabled": True,
    },
    {
        "name": "Expreso Hermosillo",
        "url": "https://www.expreso.com.mx/rss.xml",
        "scope": "sonora",
        "enabled": False,
        "note": "HTTP 404 — feed descontinuado (confirmado smoke test 2026-04-01).",
    },
    {
        "name": "El Diario de Sonora",
        "url": "https://eldiariodesonora.com.mx/rss/latest-posts",
        "scope": "sonora",
        "enabled": False,
        "note": "XML mal formado (token inválido en línea 56) — feed roto (confirmado smoke test 2026-04-01).",
    },

    # ── Nacionales ──────────────────────────────────────────────────────────
    {
        "name": "La Jornada",
        "url": "https://www.jornada.com.mx/rss/edicion.xml?v=1",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Aristegui Noticias",
        "url": "https://editorial.aristeguinoticias.com/feed/",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Aristegui - Entrevistas",
        "url": "https://editorial.aristeguinoticias.com/category/aristegui-en-vivo/entrevistas-completas/feed/",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Proceso",
        "url": "https://www.proceso.com.mx/rss/feed.html?r=9",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Animal Político",
        "url": "https://www.animalpolitico.com/feed",
        "scope": "nacional",
        "enabled": False,
        "note": "HTTP 404 — URL de feed cambió (confirmado smoke test 2026-04-01). Pendiente nueva URL.",
    },
    {
        "name": "Sin Embargo",
        "url": "https://www.sinembargo.mx/feed",
        "scope": "nacional",
        "enabled": False,
        "note": "HTTP 403 Forbidden — bloquea user-agents automatizados (confirmado smoke test 2026-04-01).",
    },
    {
        "name": "El Economista",
        "url": "https://www.eleconomista.com.mx/rss/ultimas-noticias",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Expansión",
        "url": "https://expansion.mx/rss",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Radio Fórmula",
        "url": "https://www.radioformula.com.mx/rss/feed.html?r=1",
        "scope": "nacional",
        "enabled": False,
        "note": "HTTP 404 — URL inválida (confirmado smoke test 2026-04-01). Pendiente URL correcta.",
    },
    {
        "name": "El País México",
        "url": "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/mexico/portada",
        "scope": "nacional",
        "enabled": True,
    },

    # ── Desactivados — documentados para referencia ──────────────────────────
    {
        "name": "El Universal",
        "url": "https://www.eluniversal.com.mx/arc/outboundfeeds/rss/?outputType=xml",
        "scope": "nacional",
        "enabled": True,
    },
    {
        "name": "Reforma",
        "url": "https://www.reforma.com/rss/portada.xml",
        "scope": "nacional",
        "enabled": False,
        "note": "Solo 6 entradas sin paywall — relación señal/ruido demasiado baja.",
    },
    {
        "name": "Tribuna Sonora",
        "url": "https://www.tribuna.com.mx/feed",
        "scope": "sonora",
        "enabled": True,
    },
    {
        "name": "El Sol de Hermosillo",
        "url": "https://www.elsoldehermosillo.com.mx/rss.xml",
        "scope": "sonora",
        "enabled": True,
    },
    {
        "name": "Milenio",
        "url": "https://www.milenio.com/rss",
        "scope": "nacional",
        "enabled": False,
        "note": "Feed RSS activo pero cobertura de Sonora esporádica (0-2 notas / período). Relación señal/ruido baja.",
    },
]

# ---------------------------------------------------------------------------
# FILTRO DE RELEVANCIA — aplica a feeds nacionales
# ---------------------------------------------------------------------------
SONORA_RE = re.compile(
    r"sonora|durazo|hermosillo|cajeme|nogales|guaymas|navojoa|obregón|"
    r"madres buscadoras|fgjes|gobierno estatal",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# SMOKE TEST
# ---------------------------------------------------------------------------

def test_feed(feed_url: str, timeout: int = 10) -> dict:
    """
    Valida un feed RSS antes de usarlo en producción.
    Comprueba: HTTP 200 · XML/RSS válido · al menos 1 entrada.
    Retorna dict con info básica o lanza excepción si falla.
    """
    resp = requests.get(feed_url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()                                    # HTTP 200

    parsed = feedparser.parse(resp.content)
    if parsed.bozo and parsed.bozo_exception:
        # feedparser marca bozo=1 también en feeds con charset quirks benignos;
        # solo falla si realmente no hay estructura de feed.
        if not parsed.entries:
            raise ValueError(f"XML/RSS inválido: {parsed.bozo_exception}")

    entries = parsed.entries
    if not entries:
        raise ValueError("Feed válido pero sin entradas")

    first = entries[0] if entries else {}
    return {
        "feed_title":        parsed.feed.get("title", ""),
        "entries_count":     len(entries),
        "first_entry_title": first.get("title", ""),
        "first_entry_link":  first.get("link", ""),
    }


def smoke_test_all_feeds(sources: list = None) -> list[tuple]:
    """
    Recorre RSS_SOURCES (o la lista pasada) y reporta OK / FAIL / SKIPPED.
    Retorna lista de tuplas (nombre, estado).
    """
    if sources is None:
        sources = RSS_SOURCES

    results = []
    for src in sources:
        if not src.get("enabled", True):
            note = src.get("note", "desactivado")
            results.append((src["name"], f"SKIPPED — {note}"))
            continue
        try:
            info = test_feed(src["url"])
            results.append((
                src["name"],
                f"OK — {info['entries_count']} entradas | "
                f"\"{info['first_entry_title'][:60]}\"",
            ))
        except Exception as e:
            results.append((src["name"], f"FAIL — {e}"))
        time.sleep(0.3)   # cortesía mínima entre peticiones

    return results


# ---------------------------------------------------------------------------
# GDELT
# ---------------------------------------------------------------------------

def gdelt_search(query: str, fecha_inicio: datetime = None,
                 fecha_fin: datetime = None, max_records: int = 250) -> list[dict]:
    if fecha_inicio is None:
        fecha_inicio = FECHA_INICIO
    if fecha_fin is None:
        fecha_fin = FECHA_FIN
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={quote(query)}&mode=artlist"
        f"&maxrecords={max_records}"
        f"&startdatetime={fecha_inicio.strftime('%Y%m%d%H%M%S')}"
        f"&enddatetime={fecha_fin.strftime('%Y%m%d%H%M%S')}"
        "&sourcelang=Spanish&sourcecountry=MX&format=json"
    )
    for intento in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            if intento < 2:
                print(f"  [GDELT RETRY {intento+1}] '{query}': {e}")
                time.sleep(15)
            else:
                print(f"  [GDELT ERROR] '{query}': {e}")
                return []

    rows = []
    for a in (data.get("articles") or []):
        rows.append({
            "titulo": a.get("title", ""),
            "url":    a.get("url", ""),
            "fecha":  _parse_gdelt_date(a.get("seendate", "")),
            "medio":  a.get("domain", ""),
            "fuente": "gdelt",
        })
    return rows


def _parse_gdelt_date(s: str):
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# RSS — ingesta real
# ---------------------------------------------------------------------------

def fetch_rss_source(src: dict) -> list[dict]:
    """
    Descarga un feed RSS y devuelve artículos relevantes para Sonora.
    Si el scope es "sonora" acepta todo; si es "nacional" filtra por SONORA_RE.
    """
    name  = src["name"]
    url   = src["url"]
    scope = src.get("scope", "nacional")

    try:
        feed = feedparser.parse(url)
    except Exception as e:
        print(f"  [RSS ERROR] {name}: {e}")
        return []

    rows = []
    for entry in feed.entries:
        titulo  = (entry.get("title", "") or "").strip()
        summary = entry.get("summary", "") or entry.get("description", "") or ""

        if scope == "nacional":
            if not SONORA_RE.search(titulo + " " + summary):
                continue

        published = entry.get("published_parsed") or entry.get("updated_parsed")
        fecha = datetime(*published[:6]) if published else None
        if fecha is None:
            continue   # descartamos sin fecha para no distorsionar timeline

        rows.append({
            "titulo":    titulo,
            "url":       entry.get("link", ""),
            "fecha":     fecha,
            "medio":     name,
            "fuente":    "rss",
            "contenido": re.sub(r"<[^>]+>", "", summary),
        })

    return rows


def fetch_all_rss(sources: list = None) -> list[dict]:
    """Itera RSS_SOURCES habilitados y devuelve todos los artículos."""
    if sources is None:
        sources = RSS_SOURCES

    all_rows = []
    for src in sources:
        if not src.get("enabled", True):
            continue
        print(f"  RSS [{src['scope']}] {src['name']}")
        rows = fetch_rss_source(src)
        print(f"    → {len(rows)} artículos relevantes")
        all_rows.extend(rows)
        time.sleep(0.5)

    return all_rows


# ---------------------------------------------------------------------------
# MAIN — ingesta completa
# ---------------------------------------------------------------------------

def main():
    print("=" * 55)
    print("fetch_data.py — Ingesta Sonora")
    print("=" * 55)

    all_rows = []

    print(f"\n  Periodo: {FECHA_INICIO.date()} a {FECHA_FIN.date()}")

    # ── GDELT ──
    print("\n[1/2] GDELT")
    for kw in KEYWORDS_GDELT:
        print(f"  Buscando: '{kw}'")
        rows = gdelt_search(kw, FECHA_INICIO, FECHA_FIN, MAX_GDELT_POR_QUERY)
        print(f"  → {len(rows)} artículos")
        all_rows.extend(rows)
        time.sleep(10)

    # ── RSS ──
    print("\n[2/2] RSS")
    all_rows.extend(fetch_all_rss())

    # ── DataFrame ──
    df = pd.DataFrame(all_rows)
    if df.empty:
        print("\n[!] Sin artículos. Verifica conexión o ejecuta --test primero.")
        return

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])
    df = df.drop_duplicates(subset=["url"])
    df = df.sort_values("fecha", ascending=False).reset_index(drop=True)

    out = "articulos.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"\n{'='*55}")
    print(f"[OK] {out}  →  {len(df)} artículos")
    print(f"     Período : {df['fecha'].min().date()} → {df['fecha'].max().date()}")
    print(f"     Fuentes : {df['fuente'].value_counts().to_dict()}")
    print(f"     Medios  : {df['medio'].nunique()} únicos")
    print(f"{'='*55}")
    print("Siguiente paso: python curate_data.py")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if "--test" in sys.argv:
        # Smoke test únicamente — no escribe CSV
        print("=" * 55)
        print("SMOKE TEST — validando feeds RSS")
        print("=" * 55)
        for name, status in smoke_test_all_feeds():
            icon = "OK  " if status.startswith("OK") else ("SKIP" if status.startswith("SKIP") else "FAIL")
            print(f"  [{icon}]  {name:<30} {status}")
        print("=" * 55)
    else:
        main()
