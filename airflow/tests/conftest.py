"""Make the DAG helpers importable the way Airflow makes them importable.

Airflow puts the dags folder on `sys.path`, which is why the DAGs can say
`from utils import alerting`. Replicating that here is what lets these modules be tested
without an Airflow runtime -- and `alerting` is written to allow exactly that, importing
`airflow.utils.email` lazily inside the sender rather than at module scope.
"""

from __future__ import annotations

import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
