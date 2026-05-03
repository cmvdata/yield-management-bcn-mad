# BCN-MAD Yield Management Scraper

Scraper diario de precios de vuelos **BCN↔MAD** (ambas direcciones) para construir un panel longitudinal con el que calibrar un modelo de *Yield Management*.

```
Google Flights → fast-flights (Python) → bcn_mad_prices.parquet → análisis
```

---

## Cómo se ejecuta

### Manualmente en local

```bash
pip install -r requirements.txt
python scraper.py            # sesión completa: 2 rutas × 9 DTDs = 18 queries
python scraper.py --test     # modo test: 2 rutas × 2 DTDs = 4 queries
```

El scraper devuelve **exit code `1`** si más del 50 % de las queries fallan (señal para CI).

### Automatizado (GitHub Actions)

Workflow: [`.github/workflows/daily_scrape.yml`](.github/workflows/daily_scrape.yml)

- **Cron**: 09:00 UTC todos los días
- **Trigger manual**: pestaña *Actions* → *Run workflow*
- En cada ejecución hace `commit + push` automático de `data/` y `logs/`
- Si el scraper sale con exit ≠ 0, GitHub marca la ejecución como fallida y manda email automático

### Tests

```bash
pip install pytest
pytest test_scraper.py -v
```

13 tests cubren: filtro defensivo, retry con threshold, dedup intra-sesión, acumulación append-only del parquet y manejo de exit code.

---

## Configuración del scraping

| Parámetro | Valor |
|---|---|
| Rutas | `BCN→MAD` y `MAD→BCN` |
| Horizonte (DTDs) | `[1, 3, 7, 10, 14, 21, 30, 45, 60]` días |
| Ventana rolling | hoy hasta hoy + 60 días |
| Modo | one-way · economy · 1 adulto |

---

## Estructura del parquet (`data/bcn_mad_prices.parquet`)

| Columna | Descripción |
|---|---|
| `scraped_at` | Timestamp UTC de la consulta |
| `flight_date` | Fecha del vuelo |
| `dtd` | Días hasta la salida en el momento del scraping |
| `origin` | `BCN` o `MAD` |
| `destination` | `MAD` o `BCN` |
| `airline` | Aerolínea operadora |
| `departure_time` | Hora de salida (string tal cual lo devuelve Google) |
| `arrival_time` | Hora de llegada |
| `duration` | Duración del vuelo |
| `stops` | Número de escalas (string) |
| `price_usd` | Precio numérico en USD |
| `price_raw` | Precio raw de Google (`"$66"`) |
| `is_best` | Si Google lo marca como "mejor opción" |
| `current_price_label` | `low` / `typical` / `high` según Google |

Clave compuesta de identidad de un vuelo en un instante:
```
(scraped_at, origin, destination, flight_date, departure_time, airline)
```

---

## Decisiones de diseño

### Filtro defensivo y retry
Google a veces devuelve respuestas degradadas (página de consentimiento de cookies, parser fallido) en las que las filas vienen sin `airline`. Ante eso:

1. Si una query devuelve **>50 % de filas sin airline**, esperar 30 s y reintentar una vez.
2. Si tras el retry sigue degradada, marcar la query como `failed` (no se escribe nada).
3. Si globalmente **>50 % de las queries** de la sesión fallan, el scraper sale con exit `1`.

### Deduplicación intra-sesión
En la misma respuesta Google puede listar el mismo vuelo varias veces con precios distintos (tarifas básica/flex/premium). `fast-flights` **no expone `fare_class`**, así que conservamos solo el **precio mínimo** por clave de vuelo+`scraped_at`. Esto preserva la señal "precio más bajo disponible en este momento", que es lo relevante para yield management.

Si en el futuro una versión de `fast-flights` expone `fare_class`, conviene revisar `_deduplicate_session()` en `scraper.py` para conservar todas las tarifas con esa columna.

### Append-only
Cada sesión añade filas con un `scraped_at` único; nunca se sobrescriben observaciones previas. Una segunda capa de dedup defensivo previene duplicados accidentales si una sesión se replay.

---

## Limitaciones conocidas

- **Google puede bloquear IPs de datacenter de GitHub Actions** y devolver la página de consentimiento. Cuando pasa, el filtro defensivo descarta las filas degradadas; si afecta a >50 % de queries la sesión sale con exit 1 y email automático. Si esto ocurre dos días seguidos, considerar **plan B: cron local** (un PC siempre encendido o Raspberry Pi).
- **`fast-flights` no expone `fare_class`** → el dedup intra-sesión conserva solo la tarifa más barata.
- **Hora local de los vuelos** viene como string libre de Google; el parsing fino para análisis se hace en notebook downstream.
- **Cron 09:00 UTC** = una sola foto al día. Para ver dinámica intra-día habría que duplicar el cron.

---

## Dependencias (pinned)

```
fast-flights==2.2
pandas==3.0.2
pyarrow==24.0.0
```

---

## Referencias

- Williams, K. R. (2022). *The welfare effects of dynamic pricing: Evidence from airline markets*. **Econometrica**, 90(2), 831–858.
- Lazarev, J. (2013). *The welfare effects of intertemporal price discrimination*. Stanford Working Paper.
- Jiménez, J. L., & Betancor, O. (2012). *When trains go faster than planes: The strategic reaction of airlines in Spain*. **Transport Policy**, 23, 34–41.
- Talluri, K. T., & Van Ryzin, G. J. (2004). *The Theory and Practice of Revenue Management*. Springer.
