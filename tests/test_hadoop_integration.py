from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from src.orchestration import run_analysis

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HADOOP_JAR = PROJECT_ROOT / "hadoop-jobs" / "target" / "ecommerce-sales-analytics-1.0.0.jar"
INTEGRATION_READY = (
    os.environ.get("BDA_RUN_HADOOP_INTEGRATION") == "1"
    and shutil.which(os.environ.get("HADOOP_BIN", "hadoop")) is not None
    and HADOOP_JAR.is_file()
)


@pytest.mark.skipif(not INTEGRATION_READY, reason="Requires configured HDFS, Hadoop CLI, and the built project JAR")
def test_real_hadoop_pipeline_on_hand_calculated_fixture(tmp_path):
    orders = (
        b"order_id,order_date,customer_id,product_id,quantity,unit_price,region,category\n"
        b"O-1,2025-01-01,C-1,P-1,1,2.50,North,Home\n"
        b"O-1,2025-01-01,C-1,P-1,2,1.25,North,Home\n"
    )
    products = b"product_id,product_name,category,unit_cost\nP-1,Test Mug,Home,1.00\n"

    result = run_analysis(orders, products, runs_dir=tmp_path / "runs")

    product = result["results"]["aggregates"]["product"][0]
    assert product["quantity"] == 3
    assert product["revenue_cents"] == 500
    assert product["order_count"] == 1
    assert result["results"]["joins"]["profit"][-1]["profit_cents"] == 250