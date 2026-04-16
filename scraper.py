"""
scraper.py — Scraper de precios de vuelos BCN-MAD
Proyecto: Yield Management Dinámico con Datos Reales
Autor: github.com/TU_USUARIO

Uso:
    python scraper.py                  # Ejecutar una vez manualmente
    python scraper.py --test           # Probar con una sola fecha

Automatización:
    Ver .github/workflows/scraper.yml  # GitHub Actions (gratis, 2x/día)

Dependencias:
    pip install fast-flights pandas pyarrow
"""

import os
import json
import time
import random
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from fast_flights import FlightData, Passengers, Result, get_flights

# ── Configuración ────────────────────────────────────────────────────────────
ORIGIN      = "BCN"
DESTINATION = "MAD"
DATA_DIR    = Path(__file__).parent / "data"
LOG_DIR     = Path(__file__).parent / "logs"

# Horizontes de búsqueda: cuántos días hacia adelante buscar en cada ejecución
# Esto nos da la "curva de reservas" para cada vuelo
DTD_TARGETS = [1, 3, 7, 10, 14, 21, 30, 45, 60]

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"scraper_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def scrape_date(target_date: str, dtd: int) -> list[dict]:
    """
    Consulta Google Flights para una fecha dada y devuelve lista de vuelos.
    
    Args:
        target_date: Fecha de vuelo en formato YYYY-MM-DD
        dtd: Days to Departure (días hasta la salida en el momento de la consulta)
    
    Returns:
        Lista de diccionarios con datos de cada vuelo
    """
    log.info(f"  Consultando {ORIGIN}→{DESTINATION} para {target_date} (DTD={dtd})...")

    try:
        result: Result = get_flights(
            flight_data=[
                FlightData(
                    date=target_date,
                    from_airport=ORIGIN,
                    to_airport=DESTINATION,
                )
            ],
            trip="one-way",
            seat="economy",
            passengers=Passengers(adults=1),
        )
    except Exception as e:
        log.error(f"  Error al consultar {target_date}: {e}")
        return []

    scraped_at = datetime.utcnow().isoformat()
    records = []

    for flight in result.flights:
        # Limpiar precio: "$66" → 66.0
        price_raw = getattr(flight, 'price', None)
        price_usd = None
        if price_raw:
            try:
                price_usd = float(str(price_raw).replace('$', '').replace(',', '').strip())
            except ValueError:
                price_usd = None

        records.append({
            "scraped_at":       scraped_at,
            "flight_date":      target_date,
            "dtd":              dtd,
            "airline":          getattr(flight, 'name', None),
            "departure_time":   getattr(flight, 'departure', None),
            "arrival_time":     getattr(flight, 'arrival', None),
            "duration":         getattr(flight, 'duration', None),
            "stops":            str(getattr(flight, 'stops', '')) if getattr(flight, 'stops', None) is not None else None,
            "price_usd":        price_usd,
            "price_raw":        str(price_raw) if price_raw else None,
            "is_best":          getattr(flight, 'is_best', False),
            "current_price_label": str(result.current_price) if result.current_price else None,
            "origin":           ORIGIN,
            "destination":      DESTINATION,
        })

    log.info(f"  → {len(records)} vuelos encontrados. Precio mínimo: "
             f"${min((r['price_usd'] for r in records if r['price_usd']), default='N/A')}")
    return records


def run_scraping_session(test_mode: bool = False):
    """
    Ejecuta una sesión completa de scraping para todos los DTD targets.
    Guarda los resultados en un archivo Parquet acumulativo.
    """
    today = datetime.now()
    session_id = today.strftime("%Y%m%d_%H%M%S")
    log.info(f"=== Iniciando sesión de scraping {session_id} ===")

    if test_mode:
        dtd_list = [7, 14]
        log.info("Modo TEST: solo DTD 7 y 14 días")
    else:
        dtd_list = DTD_TARGETS

    all_records = []

    for dtd in dtd_list:
        target_date = (today + timedelta(days=dtd)).strftime("%Y-%m-%d")
        records = scrape_date(target_date, dtd)
        all_records.extend(records)

        # Pausa aleatoria entre consultas para no ser bloqueados (3-8 segundos)
        if dtd != dtd_list[-1]:
            sleep_time = random.uniform(3, 8)
            log.info(f"  Esperando {sleep_time:.1f}s antes de la siguiente consulta...")
            time.sleep(sleep_time)

    if not all_records:
        log.warning("No se obtuvieron datos en esta sesión.")
        return

    # Convertir a DataFrame
    df_new = pd.DataFrame(all_records)
    df_new['scraped_at'] = pd.to_datetime(df_new['scraped_at'])
    df_new['flight_date'] = pd.to_datetime(df_new['flight_date'])

    # Guardar/acumular en Parquet
    parquet_path = DATA_DIR / "bcn_mad_prices.parquet"
    csv_path = DATA_DIR / "bcn_mad_prices.csv"

    if parquet_path.exists():
        df_existing = pd.read_parquet(parquet_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Eliminar duplicados exactos
        df_combined = df_combined.drop_duplicates(
            subset=['scraped_at', 'flight_date', 'airline', 'departure_time']
        )
    else:
        df_combined = df_new

    df_combined.to_parquet(parquet_path, index=False)
    df_combined.to_csv(csv_path, index=False)

    log.info(f"=== Sesión completada ===")
    log.info(f"  Registros nuevos:  {len(df_new)}")
    log.info(f"  Total acumulado:   {len(df_combined)}")
    log.info(f"  Guardado en:       {parquet_path}")

    # Guardar resumen de la sesión en JSON
    summary = {
        "session_id":       session_id,
        "scraped_at":       today.isoformat(),
        "new_records":      len(df_new),
        "total_records":    len(df_combined),
        "dtd_covered":      dtd_list,
        "dates_covered":    sorted(df_new['flight_date'].astype(str).unique().tolist()),
        "airlines_found":   sorted(df_new['airline'].dropna().unique().tolist()),
        "price_min_usd":    float(df_new['price_usd'].min()) if df_new['price_usd'].notna().any() else None,
        "price_max_usd":    float(df_new['price_usd'].max()) if df_new['price_usd'].notna().any() else None,
        "price_mean_usd":   round(float(df_new['price_usd'].mean()), 2) if df_new['price_usd'].notna().any() else None,
    }

    summary_path = LOG_DIR / f"summary_{session_id}.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log.info(f"  Resumen guardado:  {summary_path}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper de precios BCN-MAD")
    parser.add_argument('--test', action='store_true', help='Modo test (solo 2 fechas)')
    args = parser.parse_args()

    run_scraping_session(test_mode=args.test)
