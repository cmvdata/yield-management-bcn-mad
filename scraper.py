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
ROUTES      = [("BCN", "MAD"), ("MAD", "BCN")]
DATA_DIR    = Path(__file__).parent / "data"
LOG_DIR     = Path(__file__).parent / "logs"

# Horizontes de búsqueda: cuántos días hacia adelante buscar en cada ejecución.
# Combinado con un cron diario, estos DTDs nos dan la curva de reservas para
# cada vuelo en una ventana rolling de hasta 60 días.
DTD_TARGETS = [1, 3, 7, 10, 14, 21, 30, 45, 60]

# Defensa contra respuestas degradadas de Google Flights (consent page, parser fallido).
# Si la fracción de filas brutas sin airline supera este umbral, reintentamos.
EMPTY_AIRLINE_THRESHOLD = 0.5
RETRY_SLEEP_SECONDS = 30

# Si más de esta fracción de queries fallan en una sesión, run_scraping_session
# devuelve exit code 1 (CI marca la ejecución como fallida).
SESSION_FAILURE_EXIT_THRESHOLD = 0.5

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


def _query_flights(target_date: str, origin: str, destination: str):
    """Llamada cruda a get_flights. Devuelve el objeto Result o lanza excepción."""
    return get_flights(
        flight_data=[
            FlightData(
                date=target_date,
                from_airport=origin,
                to_airport=destination,
            )
        ],
        trip="one-way",
        seat="economy",
        passengers=Passengers(adults=1),
    )


def _is_empty_airline(flight) -> bool:
    """Una fila se considera degradada si no tiene nombre de aerolínea."""
    return not getattr(flight, 'name', None)


def _fraction_empty_airline(flights) -> float:
    """Fracción de filas sin airline. Lista vacía ⇒ 1.0 (totalmente degradada)."""
    if not flights:
        return 1.0
    return sum(1 for f in flights if _is_empty_airline(f)) / len(flights)


def _flight_to_record(flight, result, target_date: str, dtd: int,
                      scraped_at: str, origin: str, destination: str) -> dict:
    price_raw = getattr(flight, 'price', None)
    price_usd = None
    if price_raw:
        try:
            price_usd = float(str(price_raw).replace('$', '').replace(',', '').strip())
        except ValueError:
            price_usd = None

    return {
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
        "origin":           origin,
        "destination":      destination,
    }


def scrape_date(target_date: str, dtd: int,
                origin: str = "BCN", destination: str = "MAD"
                ) -> tuple[list[dict], dict]:
    """
    Consulta Google Flights para una fecha y ruta dadas, aplica filtro defensivo
    y retry si la respuesta viene degradada (>50% de filas sin airline).

    Returns:
        (records, query_log) donde:
          - records: lista filtrada (solo vuelos con airline válida). Vacía si fallido.
          - query_log: dict con timestamp, query, n_raw, n_filtered, status.
            status ∈ {"ok", "failed"}.
    """
    query_str = f"{origin}-{destination} {target_date} (DTD={dtd})"
    log.info(f"  Consultando {query_str}...")

    qlog = {
        "timestamp":  datetime.utcnow().isoformat(),
        "query":      query_str,
        "n_raw":      0,
        "n_filtered": 0,
        "status":     "failed",
    }

    # ── Intento #1 ────────────────────────────────────────────────────────
    try:
        result = _query_flights(target_date, origin, destination)
    except Exception as e:
        log.error(f"  Error al consultar {target_date}: {e}")
        log.info(f"  query_log: {qlog}")
        return [], qlog

    flights = result.flights
    empty_frac = _fraction_empty_airline(flights)

    # ── Detección + retry: respuesta degradada ────────────────────────────
    if empty_frac > EMPTY_AIRLINE_THRESHOLD:
        log.warning(
            f"  Respuesta degradada: {empty_frac:.0%} de {len(flights)} filas sin airline. "
            f"Reintentando en {RETRY_SLEEP_SECONDS}s..."
        )
        time.sleep(RETRY_SLEEP_SECONDS)
        try:
            result = _query_flights(target_date, origin, destination)
            flights = result.flights
        except Exception as e:
            log.error(f"  Error en retry: {e}")
            qlog["n_raw"] = len(flights)
            log.info(f"  query_log: {qlog}")
            return [], qlog

        empty_frac = _fraction_empty_airline(flights)
        if empty_frac > EMPTY_AIRLINE_THRESHOLD:
            log.error(
                f"  Sigue degradada tras retry ({empty_frac:.0%} vacías). Marcando como failed."
            )
            qlog["n_raw"] = len(flights)
            log.info(f"  query_log: {qlog}")
            return [], qlog

    qlog["n_raw"] = len(flights)

    # ── Filtro defensivo: descartar filas sin airline ─────────────────────
    scraped_at = datetime.utcnow().isoformat()
    records = [
        _flight_to_record(f, result, target_date, dtd, scraped_at, origin, destination)
        for f in flights
        if not _is_empty_airline(f)
    ]
    qlog["n_filtered"] = len(records)

    # ── Si tras el filtro no queda nada, sesión failed ────────────────────
    if not records:
        log.warning(f"  Tras filtrar no quedan vuelos válidos. Marcando como failed.")
        log.info(f"  query_log: {qlog}")
        return [], qlog

    qlog["status"] = "ok"
    min_price = min((r['price_usd'] for r in records if r['price_usd']), default='N/A')
    log.info(
        f"  → {len(records)} válidos / {qlog['n_raw']} brutos. Min: ${min_price}"
    )
    log.info(f"  query_log: {qlog}")
    return records, qlog


def _deduplicate_session(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dentro de una misma sesión Google puede devolver el mismo vuelo varias veces
    con precios distintos (tarifas básica/flex). `fast-flights` no expone
    `fare_class`, así que conservamos el precio mínimo por
    (scraped_at, origin, destination, flight_date, departure_time, airline).
    Decisión documentada en README.
    """
    if df.empty:
        return df
    return (df
        .sort_values("price_usd", na_position="last")
        .drop_duplicates(
            subset=["scraped_at", "origin", "destination", "flight_date",
                    "departure_time", "airline"],
            keep="first",
        )
        .reset_index(drop=True)
    )


def run_scraping_session(test_mode: bool = False) -> int:
    """
    Ejecuta una sesión completa de scraping para todas las rutas y DTDs.
    Acumula resultados en `data/bcn_mad_prices.parquet`.

    Returns:
        Exit code: 0 si la sesión es saludable, 1 si más del
        SESSION_FAILURE_EXIT_THRESHOLD de queries falló (CI marca la
        ejecución como fallida).
    """
    today = datetime.now()
    session_id = today.strftime("%Y%m%d_%H%M%S")
    log.info(f"=== Iniciando sesión de scraping {session_id} ===")

    if test_mode:
        dtd_list = [7, 14]
        log.info("Modo TEST: solo DTD 7 y 14 días")
    else:
        dtd_list = DTD_TARGETS

    # Producto cartesiano (rutas × DTDs); permite ventana rolling de hasta 60d
    queries = [(o, d, dtd) for (o, d) in ROUTES for dtd in dtd_list]
    log.info(f"  Rutas: {ROUTES} × DTDs: {dtd_list} ⇒ {len(queries)} queries")

    all_records = []
    query_logs = []

    for i, (origin, destination, dtd) in enumerate(queries):
        target_date = (today + timedelta(days=dtd)).strftime("%Y-%m-%d")
        records, qlog = scrape_date(target_date, dtd, origin, destination)
        all_records.extend(records)
        query_logs.append(qlog)

        # Pausa aleatoria entre consultas para no ser bloqueados
        if i < len(queries) - 1:
            sleep_time = random.uniform(3, 8)
            log.info(f"  Esperando {sleep_time:.1f}s...")
            time.sleep(sleep_time)

    queries_ok = sum(1 for q in query_logs if q["status"] == "ok")
    queries_failed = len(query_logs) - queries_ok
    failure_rate = queries_failed / len(query_logs) if query_logs else 1.0

    parquet_path = DATA_DIR / "bcn_mad_prices.parquet"
    csv_path = DATA_DIR / "bcn_mad_prices.csv"

    # ── Caso totalmente fallido: no se toca el panel ──────────────────────
    if not all_records:
        log.warning(
            f"Sin datos en esta sesión. "
            f"({queries_failed}/{len(query_logs)} queries fallidas)"
        )
        summary_path = LOG_DIR / f"summary_{session_id}.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                "session_id":      session_id,
                "scraped_at":      today.isoformat(),
                "status":          "failed",
                "new_records":     0,
                "queries_ok":      queries_ok,
                "queries_failed":  queries_failed,
                "failure_rate":    round(failure_rate, 3),
                "query_logs":      query_logs,
            }, f, indent=2, ensure_ascii=False)
        log.info(f"  Resumen (failed) guardado: {summary_path}")
        return 1  # sin datos = crítico

    # ── DataFrame, dedup intra-sesión, persistencia ───────────────────────
    df_new = pd.DataFrame(all_records)
    df_new['scraped_at']  = pd.to_datetime(df_new['scraped_at'])
    df_new['flight_date'] = pd.to_datetime(df_new['flight_date'])

    n_pre_dedup = len(df_new)
    df_new = _deduplicate_session(df_new)
    n_dedup_drops = n_pre_dedup - len(df_new)
    log.info(f"  Dedup intra-sesión: {n_pre_dedup} → {len(df_new)} "
             f"({n_dedup_drops} fares duplicados colapsados)")

    if parquet_path.exists():
        df_existing = pd.read_parquet(parquet_path)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        # Defensa contra reejecuciones accidentales (mismo scraped_at + clave de vuelo)
        df_combined = df_combined.drop_duplicates(
            subset=['scraped_at', 'origin', 'destination',
                    'flight_date', 'airline', 'departure_time']
        ).reset_index(drop=True)
    else:
        df_combined = df_new

    df_combined.to_parquet(parquet_path, index=False)
    df_combined.to_csv(csv_path, index=False)

    log.info(f"=== Sesión completada ===")
    log.info(f"  Queries ok / failed: {queries_ok} / {queries_failed}")
    log.info(f"  Registros nuevos:    {len(df_new)}")
    log.info(f"  Total acumulado:     {len(df_combined)}")
    log.info(f"  Guardado en:         {parquet_path}")

    summary = {
        "session_id":       session_id,
        "scraped_at":       today.isoformat(),
        "status":           "ok" if queries_failed == 0 else "partial",
        "new_records":      len(df_new),
        "dedup_drops":      n_dedup_drops,
        "total_records":    len(df_combined),
        "queries_ok":       queries_ok,
        "queries_failed":   queries_failed,
        "failure_rate":     round(failure_rate, 3),
        "query_logs":       query_logs,
        "routes_covered":   ["-".join(r) for r in ROUTES],
        "dtd_covered":      dtd_list,
        "dates_covered":    sorted(df_new['flight_date'].astype(str).unique().tolist()),
        "airlines_found":   sorted(df_new['airline'].dropna().unique().tolist()),
        "price_min_usd":    float(df_new['price_usd'].min()) if df_new['price_usd'].notna().any() else None,
        "price_max_usd":    float(df_new['price_usd'].max()) if df_new['price_usd'].notna().any() else None,
        "price_mean_usd":   round(float(df_new['price_usd'].mean()), 2) if df_new['price_usd'].notna().any() else None,
    }

    summary_path = LOG_DIR / f"summary_{session_id}.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log.info(f"  Resumen guardado:    {summary_path}")

    # Exit code: si más del umbral de queries falló, indicar fallo crítico a CI
    if failure_rate > SESSION_FAILURE_EXIT_THRESHOLD:
        log.error(
            f"  Failure rate {failure_rate:.0%} > umbral {SESSION_FAILURE_EXIT_THRESHOLD:.0%}. "
            "Marcando ejecución como fallida (exit 1)."
        )
        return 1
    return 0


if __name__ == "__main__":
    import sys
    parser = argparse.ArgumentParser(description="Scraper de precios BCN-MAD / MAD-BCN")
    parser.add_argument('--test', action='store_true', help='Modo test (solo 2 DTDs)')
    args = parser.parse_args()

    sys.exit(run_scraping_session(test_mode=args.test))
