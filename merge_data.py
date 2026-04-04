"""
merge_data.py
Une articulos.csv (noticias) + social.csv (redes sociales)
y guarda el resultado en articulos.csv.

Uso:
    python merge_data.py
"""

import pandas as pd
import os

NOTICIAS_FILE = "articulos.csv"
SOCIAL_FILE   = "social.csv"
OUTPUT_FILE   = "articulos.csv"
COLUMNS       = ["titulo", "url", "fecha", "medio", "fuente"]


def load(path):
    if not os.path.exists(path):
        print(f"  [!] No encontrado: {path} — se omite")
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_csv(path, encoding="utf-8-sig")
    # Asegurar que existan todas las columnas
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[COLUMNS]


def main():
    print("=" * 50)
    print("merge_data.py — Uniendo datasets")
    print("=" * 50)

    df_noticias = load(NOTICIAS_FILE)
    print(f"  articulos.csv : {len(df_noticias)} registros")

    df_social = load(SOCIAL_FILE)
    print(f"  social.csv    : {len(df_social)} registros")

    df = pd.concat([df_noticias, df_social], ignore_index=True)

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    # YouTube sin fecha se conserva — el usuario acepta incluirlos sin fecha
    # Solo se descartan registros sin fecha que NO sean YouTube
    mask_sin_fecha = df["fecha"].isna()
    mask_no_youtube = df["fuente"].str.lower() != "youtube"
    descartar = mask_sin_fecha & mask_no_youtube
    sin_fecha = descartar.sum()
    if sin_fecha:
        print(f"  [!] {sin_fecha} registros descartados por fecha invalida o vacia")
    df = df[~descartar]
    df = df.drop_duplicates(subset=["url"])
    df = df.sort_values("fecha", ascending=False).reset_index(drop=True)

    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\n[OK] Guardado: {OUTPUT_FILE}  ({len(df)} registros totales)")
    print(f"  Fuentes: {df['fuente'].value_counts().to_dict()}")
    print(f"  Medios únicos: {df['medio'].nunique()}")
    if df['fecha'].notna().any():
        fechas_validas = df['fecha'].dropna()
        if len(fechas_validas):
            print(f"  Periodo: {fechas_validas.min()} a {fechas_validas.max()}")
    print("=" * 50)


if __name__ == "__main__":
    main()
