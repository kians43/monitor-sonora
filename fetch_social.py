"""
fetch_social.py
Obtiene datos de YouTube y X (vía Nitter RSS) sobre Sonora/Durazo.

Uso:
    python fetch_social.py

Genera: social.csv
"""

import csv
import time
import re
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
KEYWORDS = ["Sonora", "Durazo", "Gobierno de Sonora"]
OUTPUT_FILE = "social.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (research-project/1.0)"}

# Instancias públicas de Nitter (se prueban en orden hasta que una funcione)
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.cz",
    "https://nitter.nl",
    "https://nitter.it",
    "https://bird.trom.tf",
    "https://nitter.mint.lgbt",
    "https://nitter.unixfox.eu",
    "https://nitter.moomoo.me",
    "https://nitter.privacydev.net",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
]

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def fetch_url(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as e:
        print(f"    [!] {url}: {e}")
        return None


def parse_date(date_str):
    if not date_str:
        return ""
    date_str = date_str.strip()
    fmts = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return datetime.strptime(date_str[:16], "%Y-%m-%dT%H:%M").strftime("%Y-%m-%d")
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# YOUTUBE  (yt-dlp — sin API key, activamente mantenido)
# ---------------------------------------------------------------------------

def fetch_youtube(keywords):
    try:
        import yt_dlp
    except ImportError:
        print("  [!] Instala yt-dlp:  pip install yt-dlp")
        return []

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        # extract_flat omite metadatos individuales (incluyendo upload_date).
        # Sin él, yt_dlp consulta cada video y devuelve la fecha real de subida.
        "skip_download": True,
        "ignoreerrors": True,
    }

    rows = []
    for kw in keywords:
        print(f"  YouTube: buscando '{kw}'...")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(f"ytsearch20:{kw}", download=False)
            videos = result.get("entries", []) or []
            for v in videos:
                vid_id = v.get("id", "")
                titulo = (v.get("title", "") or "").strip()
                url    = f"https://www.youtube.com/watch?v={vid_id}" if vid_id else ""
                # upload_date viene como "20240315" cuando está disponible
                raw_date = v.get("upload_date", "") or ""
                try:
                    fecha = datetime.strptime(raw_date, "%Y%m%d").strftime("%Y-%m-%d") if raw_date else ""
                except Exception:
                    fecha = ""

                if titulo and url:
                    rows.append({
                        "titulo": titulo,
                        "url":    url,
                        "fecha":  fecha,
                        "medio":  "YouTube",
                        "fuente": "youtube",
                    })
            print(f"    -> {len(videos)} videos")
        except Exception as e:
            print(f"    [!] Error YouTube '{kw}': {e}")
        time.sleep(1)
    return rows


# ---------------------------------------------------------------------------
# X / TWITTER  (vía Nitter RSS — sin API de pago)
# ---------------------------------------------------------------------------

def get_nitter_base():
    """Devuelve la primera instancia de Nitter que responde sin bloquear búsquedas."""
    for instance in NITTER_INSTANCES:
        # Probar directamente con una búsqueda real, no solo conectividad
        data = fetch_url(f"{instance}/search/rss?q=Mexico&f=tweets", timeout=10)
        if data and b"<item>" in data:
            print(f"  Nitter activo: {instance}")
            return instance
    return None


def fetch_nitter(keywords):
    rows = []
    base = get_nitter_base()
    if not base:
        print("  [!] Ninguna instancia Nitter disponible. Saltando X.")
        return rows

    for kw in keywords:
        encoded = urllib.parse.quote(kw)
        url = f"{base}/search/rss?q={encoded}&f=tweets"
        print(f"  X/Nitter: buscando '{kw}'...")
        data = fetch_url(url)
        if not data:
            continue
        try:
            root = ET.fromstring(data)
            items = root.findall(".//item")
            count = 0
            for item in items:
                titulo_el = item.find("title")
                link_el   = item.find("link")
                date_el   = item.find("pubDate")

                titulo = (titulo_el.text or "").strip() if titulo_el is not None else ""
                link   = (link_el.text or "").strip()   if link_el   is not None else ""
                fecha  = parse_date(date_el.text)        if date_el   is not None else ""

                # Convertir URLs de Nitter a Twitter
                link = re.sub(r"https?://[^/]+/", "https://twitter.com/", link, count=1)

                if titulo and link:
                    rows.append({
                        "titulo": titulo,
                        "url": link,
                        "fecha": fecha,
                        "medio": "X",
                        "fuente": "x",
                    })
                    count += 1
            print(f"    -> {count} tweets")
        except Exception as e:
            print(f"    [!] Error parseando Nitter '{kw}': {e}")
        time.sleep(1)
    return rows


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def dedup(rows):
    seen, unique = set(), []
    for r in rows:
        if r["url"] not in seen:
            seen.add(r["url"])
            unique.append(r)
    return unique


def save_csv(rows, path):
    fieldnames = ["titulo", "url", "fecha", "medio", "fuente"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n[OK] Guardado: {path}  ({len(rows)} registros)")


def main():
    print("=" * 50)
    print("fetch_social.py — YouTube + X")
    print("=" * 50)

    all_rows = []

    print("\n[1/2] YouTube")
    yt_rows = fetch_youtube(KEYWORDS)
    all_rows.extend(yt_rows)
    print(f"  Subtotal YouTube: {len(yt_rows)}")

    print("\n[2/2] X (Nitter RSS)")
    x_rows = fetch_nitter(KEYWORDS)
    all_rows.extend(x_rows)
    print(f"  Subtotal X: {len(x_rows)}")

    all_rows = dedup(all_rows)
    save_csv(all_rows, OUTPUT_FILE)
    print("=" * 50)


if __name__ == "__main__":
    main()
