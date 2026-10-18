from __future__ import annotations

import json

import pytest

from src.orchestration import PipelineFailure, load_recent_runs, run_analysis


def test_hadoop_failure_is_persisted_without_stale_analytics(tmp_path):
    orders = (
        b"order_id,order_date,customer_id,product_id,quantity,unit_price,region,category\n"
        b"O-1,2025-01-01,C-1,P-1,1,5.00,North,Home\n"
    )

    with pytest.raises(PipelineFailure) as caught:
        run_analysis(
            orders,
            hadoop="hadoop-command-that-does-not-exist",
            jar_path=tmp_path / "missing.jar",
            runs_dir=tmp_path / "runs",
        )

    result = caught.value.result
    saved = next((tmp_path / "runs").glob("*/run.json"))
    persisted = json.loads(saved.read_text(encoding="utf-8"))
    assert result["status"] == persisted["status"] == "FAILED"
    assert result["results"] is None
    assert "not found" in result["error"].lower()
    history = load_recent_runs(tmp_path / "runs", limit=1)
    assert history[0]["status"] == "FAILED"
    assert history[0]["error"] == persisted["error"]


def test_filters_are_applied_before_hdfs_without_computing_metrics(tmp_path, monkeypatch):
    orders = (
        b"order_id,order_date,customer_id,product_id,quantity,unit_price,region,category\n"
        b"O-1,2025-01-01,C-1,P-1,1,5.00,North,Home\n"
        b"O-2,2025-02-01,C-2,P-2,1,7.00,South,Toys\n"
    )
    uploaded = []

    def stop_after_filter(self, local_path, hdfs_directory, filename):
        uploaded.append((filename, local_path.read_text(encoding="utf-8")))
        raise RuntimeError("stop before MapReduce")

    monkeypatch.setattr("src.orchestration.HDFSClient.upload", stop_after_filter)
    with pytest.raises(PipelineFailure):
        run_analysis(
            orders,
            hadoop="hadoop",
            jar_path=tmp_path / "missing.jar",
            runs_dir=tmp_path / "runs",
            filters={"start_date": "2025-02-01", "end_date": "2025-02-28", "region": "South", "category": "Toys"},
        )

    staged_orders = next(text for filename, text in uploaded if filename == "orders.csv")
    assert staged_orders.count("\n") == 2
    assert "O-2" in staged_orders
    assert "O-1" not in staged_orders