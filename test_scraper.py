"""
test_scraper.py — Tests del scraper.

Cubre:
  - Filtro defensivo (filas sin airline)
  - Retry cuando >50% de la respuesta viene degradada
  - Dedup intra-sesión (fares múltiples del mismo vuelo)
  - Acumulación append-only del parquet entre sesiones

Ejecutar:
    pytest test_scraper.py -v
"""

from unittest.mock import MagicMock

import pandas as pd

import scraper


def make_flight(name="Iberia", price="$100", departure="9:00 AM",
                arrival="10:25 AM", duration="1 hr 25 min", stops=0,
                is_best=False):
    f = MagicMock()
    f.name = name
    f.price = price
    f.departure = departure
    f.arrival = arrival
    f.duration = duration
    f.stops = stops
    f.is_best = is_best
    return f


def make_result(flights, current_price="typical"):
    r = MagicMock()
    r.flights = flights
    r.current_price = current_price
    return r


# ── PASO 1: filtro defensivo ────────────────────────────────────────────────

def test_filter_drops_empty_airline_rows(monkeypatch):
    """Filas sin airline se descartan; solo las válidas llegan al output."""
    flights = [
        make_flight(name="Iberia"),
        make_flight(name="", price="$66"),     # degradada
        make_flight(name=None, price="$70"),   # degradada
        make_flight(name="Vueling"),
    ]
    monkeypatch.setattr(scraper, "get_flights",
                        MagicMock(return_value=make_result(flights)))

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert qlog["status"] == "ok"
    assert qlog["n_raw"] == 4
    assert qlog["n_filtered"] == 2
    assert len(records) == 2
    assert all(r["airline"] for r in records)
    assert {r["airline"] for r in records} == {"Iberia", "Vueling"}


def test_degraded_input_returns_zero_rows_not_empty_rows(monkeypatch):
    """
    Spec: 'Verificar que input degradado produce 0 filas, no filas vacías.'
    Si todas las filas vienen sin airline, se reintenta una vez; si sigue
    igual, devuelve [] (no filas placeholder).
    """
    flights = [make_flight(name="", price=f"${i}") for i in range(10)]
    mock_get = MagicMock(return_value=make_result(flights))
    monkeypatch.setattr(scraper, "get_flights", mock_get)
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert records == []                     # no filas, no placeholders
    assert qlog["status"] == "failed"
    assert qlog["n_filtered"] == 0
    assert mock_get.call_count == 2          # primer intento + retry


def test_session_failed_when_no_valid_records(monkeypatch):
    """
    Si tras filtrar la lista queda vacía, el status es 'failed' y no se
    devuelven records que el caller pueda escribir al panel.
    """
    # Una sola fila pero sin airline ⇒ tras filtrar queda 0
    # Y como _fraction_empty_airline = 1.0 > 0.5, dispara retry primero
    flights = [make_flight(name="")]
    monkeypatch.setattr(scraper, "get_flights",
                        MagicMock(return_value=make_result(flights)))
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert records == []
    assert qlog["status"] == "failed"


# ── PASO 2: detección + retry con threshold 50% ─────────────────────────────

def test_retry_triggers_above_threshold(monkeypatch):
    """6/10 filas vacías (60%) ⇒ se dispara retry."""
    degraded = ([make_flight(name="") for _ in range(6)] +
                [make_flight(name="Iberia") for _ in range(4)])
    healthy = [make_flight(name=f"Airline{i}") for i in range(10)]

    mock_get = MagicMock(side_effect=[make_result(degraded), make_result(healthy)])
    monkeypatch.setattr(scraper, "get_flights", mock_get)
    sleep_calls = []
    monkeypatch.setattr(scraper.time, "sleep", lambda s: sleep_calls.append(s))

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert mock_get.call_count == 2
    assert scraper.RETRY_SLEEP_SECONDS in sleep_calls
    assert qlog["status"] == "ok"
    assert qlog["n_raw"] == 10
    assert qlog["n_filtered"] == 10          # ahora todas válidas


def test_no_retry_at_threshold_boundary(monkeypatch):
    """Exactamente 50% vacías NO dispara retry (umbral es '> 50%', no '>=')."""
    flights = ([make_flight(name="") for _ in range(5)] +
               [make_flight(name="Iberia") for _ in range(5)])
    mock_get = MagicMock(return_value=make_result(flights))
    monkeypatch.setattr(scraper, "get_flights", mock_get)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert mock_get.call_count == 1          # sin retry
    assert qlog["status"] == "ok"
    assert qlog["n_filtered"] == 5


def test_no_retry_when_response_is_clean(monkeypatch):
    """0% vacías ⇒ ni retry ni warning."""
    flights = [make_flight(name=f"Airline{i}") for i in range(10)]
    mock_get = MagicMock(return_value=make_result(flights))
    monkeypatch.setattr(scraper, "get_flights", mock_get)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert mock_get.call_count == 1
    assert qlog["status"] == "ok"
    assert qlog["n_raw"] == 10
    assert qlog["n_filtered"] == 10


def test_retry_persists_failure_marks_failed(monkeypatch):
    """Si tras el retry sigue >50% vacío, status=failed y records=[]."""
    degraded = [make_flight(name="") for _ in range(8)] + \
               [make_flight(name="Iberia") for _ in range(2)]
    still_degraded = [make_flight(name="") for _ in range(7)] + \
                     [make_flight(name="Iberia") for _ in range(3)]

    mock_get = MagicMock(side_effect=[make_result(degraded),
                                      make_result(still_degraded)])
    monkeypatch.setattr(scraper, "get_flights", mock_get)
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert records == []
    assert qlog["status"] == "failed"
    assert mock_get.call_count == 2


# ── PASO 3: log estructurado tiene los campos requeridos ────────────────────

def test_query_log_has_required_fields(monkeypatch):
    """timestamp, query, n_raw, n_filtered, status."""
    flights = [make_flight(name="Iberia")]
    monkeypatch.setattr(scraper, "get_flights",
                        MagicMock(return_value=make_result(flights)))

    _, qlog = scraper.scrape_date("2026-05-10", 7)

    assert set(qlog.keys()) >= {"timestamp", "query", "n_raw", "n_filtered", "status"}
    assert qlog["query"] == "BCN-MAD 2026-05-10 (DTD=7)"
    assert qlog["status"] == "ok"


def test_exception_in_first_call_returns_failed(monkeypatch):
    """get_flights() lanza ⇒ status=failed, records=[], sin retry."""
    mock_get = MagicMock(side_effect=RuntimeError("No flights found"))
    monkeypatch.setattr(scraper, "get_flights", mock_get)

    records, qlog = scraper.scrape_date("2026-05-10", 7)

    assert records == []
    assert qlog["status"] == "failed"
    assert mock_get.call_count == 1


# ── PASO 5: dedup intra-sesión ──────────────────────────────────────────────

def _row(price, departure="9:00 AM", airline="Iberia",
         scraped_at="2026-05-03T09:00:00", origin="BCN", destination="MAD",
         flight_date="2026-05-10"):
    """Construye un registro con campos mínimos para tests de dedup."""
    return {
        "scraped_at":     scraped_at,
        "flight_date":    flight_date,
        "dtd":            7,
        "airline":        airline,
        "departure_time": departure,
        "arrival_time":   "10:25 AM",
        "duration":       "1 hr 25 min",
        "stops":          "0",
        "price_usd":      price,
        "price_raw":      f"${price}",
        "is_best":        False,
        "current_price_label": "typical",
        "origin":         origin,
        "destination":    destination,
    }


def test_dedup_keeps_min_price_for_duplicate_fares():
    """
    Mismo vuelo (misma clave de identidad) con varios precios en la misma
    sesión ⇒ solo sobrevive la fila con precio mínimo.
    """
    df = pd.DataFrame([
        _row(price=234),
        _row(price=234),  # duplicado exacto
        _row(price=311),  # tarifa flex
        _row(price=319),  # tarifa premium
    ])
    out = scraper._deduplicate_session(df)

    assert len(out) == 1
    assert out.iloc[0]["price_usd"] == 234


def test_dedup_does_not_collapse_distinct_flights():
    """Vuelos distintos (otra hora, otra airline, otra ruta) NO se colapsan."""
    df = pd.DataFrame([
        _row(price=100, departure="9:00 AM"),
        _row(price=120, departure="11:00 AM"),                       # otra hora
        _row(price=150, airline="Vueling"),                          # otra airline
        _row(price=200, origin="MAD", destination="BCN"),            # otra ruta
        _row(price=180, scraped_at="2026-05-03T15:00:00"),           # otra sesión
    ])
    out = scraper._deduplicate_session(df)

    assert len(out) == 5  # ninguno se colapsa


def test_parquet_accumulates_across_sessions(monkeypatch, tmp_path):
    """
    Re-ejecutar run_scraping_session NO sobrescribe: el parquet acumula entre
    sesiones (append-only por scraped_at) y no quedan duplicados exactos.
    """
    monkeypatch.setattr(scraper, "DATA_DIR", tmp_path)
    monkeypatch.setattr(scraper, "LOG_DIR", tmp_path)
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    # Cada llamada a scrape_date devuelve 1 registro con scraped_at único
    counter = {"n": 0}
    def fake_scrape(target_date, dtd, origin="BCN", destination="MAD"):
        counter["n"] += 1
        rec = _row(
            price=100 + counter["n"],
            scraped_at=f"2026-05-03T00:00:{counter['n']:02d}",
            origin=origin, destination=destination,
            flight_date=target_date,
        )
        rec["dtd"] = dtd
        qlog = {"timestamp": "now", "query": f"{origin}-{destination} {target_date}",
                "n_raw": 1, "n_filtered": 1, "status": "ok"}
        return [rec], qlog
    monkeypatch.setattr(scraper, "scrape_date", fake_scrape)

    parquet_path = tmp_path / "bcn_mad_prices.parquet"

    # Sesión 1
    rc1 = scraper.run_scraping_session(test_mode=True)
    assert rc1 == 0
    df1 = pd.read_parquet(parquet_path)
    n1 = len(df1)
    assert n1 > 0

    # Sesión 2 — debe ACUMULAR sobre la primera, no reemplazarla
    rc2 = scraper.run_scraping_session(test_mode=True)
    assert rc2 == 0
    df2 = pd.read_parquet(parquet_path)
    assert len(df2) == n1 * 2, "El parquet debe acumular append-only"
    # No duplicados exactos
    assert df2.duplicated().sum() == 0, "No debe haber filas exactamente duplicadas"
    # No duplicados por la clave compuesta
    assert df2.duplicated(
        subset=["scraped_at", "origin", "destination",
                "flight_date", "airline", "departure_time"]
    ).sum() == 0


def test_session_failure_above_threshold_returns_exit_code_1(monkeypatch, tmp_path):
    """Si >50% de queries fallan (sin records), run_scraping_session devuelve 1."""
    monkeypatch.setattr(scraper, "DATA_DIR", tmp_path)
    monkeypatch.setattr(scraper, "LOG_DIR", tmp_path)
    monkeypatch.setattr(scraper.time, "sleep", lambda _: None)

    def all_fail(target_date, dtd, origin="BCN", destination="MAD"):
        return [], {"timestamp": "now", "query": f"{origin}-{destination} {target_date}",
                    "n_raw": 0, "n_filtered": 0, "status": "failed"}
    monkeypatch.setattr(scraper, "scrape_date", all_fail)

    rc = scraper.run_scraping_session(test_mode=True)
    assert rc == 1
