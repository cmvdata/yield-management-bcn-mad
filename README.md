# ✈️ BCN-MAD Yield Management Scraper

**Scraper de precios de vuelos Barcelona-Madrid — 100% gratuito, sin API key, automatizado con GitHub Actions.**

Este repositorio recolecta automáticamente precios de vuelos BCN→MAD dos veces al día durante 60 días para construir un panel de datos longitudinal con el que calibrar un modelo de *Yield Management* dinámico.

---

## ¿Cómo funciona?

El scraper usa [`fast-flights`](https://github.com/AWeirdDev/flights), una librería Python que consulta Google Flights directamente mediante Protobuf (sin API key, sin coste). GitHub Actions lo ejecuta gratis dos veces al día y hace commit automático de los datos al repositorio.

```
Google Flights ──► fast-flights (Python) ──► bcn_mad_prices.parquet ──► Notebooks de análisis
```

---

## Estructura del repositorio

```
yield-management-bcn-mad/
├── scraper.py                          ← Script principal de scraping
├── requirements.txt
├── .github/
│   └── workflows/
│       └── scraper.yml                 ← GitHub Actions (2x/día, gratis)
├── data/
│   ├── bcn_mad_prices.parquet          ← Panel acumulativo (se actualiza solo)
│   └── bcn_mad_prices.csv              ← Mismos datos en CSV
├── logs/
│   └── summary_YYYYMMDD_HHMMSS.json   ← Resumen de cada sesión
└── notebooks/
    ├── 01_EDA_Price_Dynamics.ipynb
    ├── 02_Booking_Curve_Estimation.ipynb
    ├── 03_Intermodal_Competition.ipynb
    └── 04_Yield_Optimization.ipynb
```

---

## Configuración (5 minutos)

### 1. Fork o clona este repositorio

```bash
git clone https://github.com/TU_USUARIO/yield-management-bcn-mad.git
cd yield-management-bcn-mad
```

### 2. Activa GitHub Actions

Ve a tu repositorio en GitHub → pestaña **Actions** → haz clic en **"I understand my workflows, go ahead and enable them"**.

Eso es todo. El scraper se ejecutará automáticamente a las **09:00 y 19:00 (hora de Madrid)** todos los días.

### 3. (Opcional) Ejecutar manualmente

```bash
pip install -r requirements.txt
python scraper.py          # Ejecución completa (9 fechas)
python scraper.py --test   # Modo test (solo 2 fechas, ~30 segundos)
```

---

## Datos recolectados

Cada ejecución consulta los precios para vuelos con **1, 3, 7, 10, 14, 21, 30, 45 y 60 días de antelación**. Esto permite observar la evolución del precio para cada vuelo a medida que se acerca la fecha de salida.

| Columna | Descripción |
|---|---|
| `scraped_at` | Timestamp UTC de la consulta |
| `flight_date` | Fecha del vuelo |
| `dtd` | Days to Departure en el momento de la consulta |
| `airline` | Aerolínea operadora |
| `departure_time` | Hora de salida |
| `arrival_time` | Hora de llegada |
| `duration` | Duración del vuelo |
| `stops` | Número de escalas |
| `price_usd` | Precio en USD (numérico) |
| `is_best` | Si Google lo marca como "mejor opción" |
| `current_price_label` | Etiqueta de precio de Google (typical/low/high) |

---

## Modelo de análisis

Con 60 días de datos (~50.000 observaciones) se estiman cuatro modelos:

1. **Regresión hedónica** — `ln(precio) = f(DTD, día_semana, aerolínea, escalas)`
2. **Booking Curve** — Logística inversa para estimar la ocupación implícita
3. **WTP Estimation** — Modelo de elección discreta para segmentar pasajeros
4. **Competencia intermodal** — Diff-in-Diff para medir el efecto del AVE en los precios aéreos

---

## Dependencias

```
fast-flights>=2.2.0
pandas>=2.0.0
pyarrow>=14.0.0
```

---

## Referencias académicas

- Williams, K. R. (2022). *The welfare effects of dynamic pricing: Evidence from airline markets*. **Econometrica**, 90(2), 831–858.
- Lazarev, J. (2013). *The welfare effects of intertemporal price discrimination*. Stanford Working Paper.
- Jiménez, J. L., & Betancor, O. (2012). *When trains go faster than planes: The strategic reaction of airlines in Spain*. **Transport Policy**, 23, 34–41.
- Talluri, K. T., & Van Ryzin, G. J. (2004). *The Theory and Practice of Revenue Management*. Springer.

---

*Proyecto de portafolio — Economía Industrial Aplicada y Data Science*
