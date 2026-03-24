from __future__ import annotations

import sys
from pathlib import Path

CONTAINER_PROJECT_ROOT = Path("/opt/project")
LOCAL_PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = CONTAINER_PROJECT_ROOT if CONTAINER_PROJECT_ROOT.exists() else LOCAL_PROJECT_ROOT
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse.orchestration.sales_pipeline import build_sales_pipeline_dag

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ModuleNotFoundError:
    dag = None
    airflow_tasks = []
else:
    dag, airflow_tasks = build_sales_pipeline_dag(DAG, BashOperator, project_root="/opt/project")
