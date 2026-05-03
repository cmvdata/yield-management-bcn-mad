"""
scripts/scrape_bcn_mad.py — Entry point para la ejecución diaria (CI o cron local).

Importa el módulo `scraper` del directorio raíz, ejecuta una sesión completa
y propaga su exit code. Cualquier fallo crítico (>50% de queries failed) se
reporta a CI vía exit 1.

Uso (manual desde la raíz del repo):
    python scripts/scrape_bcn_mad.py
"""

import sys
from pathlib import Path

# Permitir importar el módulo `scraper` desde la raíz del repo
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scraper import run_scraping_session  # noqa: E402

if __name__ == "__main__":
    sys.exit(run_scraping_session())
