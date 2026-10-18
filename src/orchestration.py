"""Validate, stage, run, and persist one isolated Hadoop analytics run."""

from __future__ import annotations

import csv
import json
import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

from src.config import load_settings
from src.hadoop.hdfs_client import HDFSClient
from src.hadoop.job_runner import HadoopJobRunner, JobExecution
from src.hadoop.output_reader import OutputReader
from src.ingestion.validation import ValidationReport, validate_orders, validate_products

ORDERS_MAIN = "com.bda.sales.SalesAggregation"
RANKING_MAIN = "com.bda.sales.ProductRanking"
JOIN_MAIN = "com.bda.sales.ProductJoin"
LOGGER = logging.getLogger("bda.orchestration")


class PipelineFailure(RuntimeError):
    def __init__(self, message: str, result: dict[str, Any]):
        self.result = result
        super().__init__(message)


def _report_dict(report: ValidationReport) -> dict[str, Any]:
    return {
        "total_rows": report.total_rows,
        "accepted_rows": report.accepted_rows,
        "rejected_rows": report.rejected_rows,
        "issues": [{"row_number": issue.row_number, "message": issue.message} for issue in report.issues],
    }


def _write_products(report: ValidationReport | None, orders: ValidationReport, destination: Path) -> tuple[bool, int]:
    if report is None:
        products: dict[str, str] = {}
        for row in orders.clean_rows:
            products.setdefault(row["product_id"], row["category"])
        headers = ["product_id", "product_name", "category"]
        rows = [
            {"product_id": product_id, "product_name": product_id, "category": category}
            for product_id, category in products.items()
        ]
        has_unit_cost = False
        product_count = len(rows)
    else:
        has_unit_cost = "unit_cost" in report.headers
        headers = ["product_id", "product_name", "category"] + (["unit_cost"] if has_unit_cost else [])
        rows = [{key: row.get(key, "") for key in headers} for row in report.clean_rows]
        product_count = report.total_rows
    with destination.open("w", encoding="utf-8", newline="") as product_file:
        writer = csv.DictWriter(product_file, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return has_unit_cost, product_count


def _apply_filters(rows: list[dict[str, str]], filters: dict[str, Any] | None) -> list[dict[str, str]]:
    if not filters:
        return rows
    start_date = date.fromisoformat(filters["start_date"]) if filters.get("start_date") else None
    end_date = date.fromisoformat(filters["end_date"]) if filters.get("end_date") else None
    if start_date and end_date and end_date < start_date:
        raise ValueError("The selected end date must not precede the start date.")
    region = filters.get("region")
    category = filters.get("category")
    return [
        row
        for row in rows
        if (start_date is None or date.fromisoformat(row["order_date"]) >= start_date)
        and (end_date is None or date.fromisoformat(row["order_date"]) <= end_date)
        and (not region or row["region"] == region)
        and (not category or row["category"] == category)
    ]


def _save_result(run_dir: Path, result: dict[str, Any]) -> None:
    temporary = run_dir / "run.json.tmp"
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(run_dir / "run.json")
    history = {
        key: result.get(key)
        for key in ("run_id", "status", "started_at", "finished_at", "error", "hdfs_paths", "validation")
    }
    history["jobs"] = [
        {key: job.get(key) for key in ("name", "status", "duration_seconds", "output_path", "error")}
        for job in result.get("jobs", [])
    ]
    (run_dir / "history.json.tmp").write_text(json.dumps(history, indent=2), encoding="utf-8")
    (run_dir / "history.json.tmp").replace(run_dir / "history.json")


def load_recent_runs(runs_dir: str | Path, limit: int = 5) -> list[dict[str, Any]]:
    """Load compact status records without restoring analytics from an old run."""
    records = []
    for history_path in Path(runs_dir).glob("*/history.json"):
        try:
            records.append(json.loads(history_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    records.sort(key=lambda record: record.get("finished_at") or "", reverse=True)
    return records[:limit]


def _validate_profit_line_ranges(rows: list[dict[str, str]], product_report: ValidationReport | None) -> None:
    if product_report is None or "unit_cost" not in product_report.headers:
        return
    costs = {row["product_id"]: int(Decimal(row["unit_cost"]) * 100) for row in product_report.clean_rows}
    for row in rows:
        if row["product_id"] not in costs:
            continue
        quantity = int(row["quantity"])
        price_cents = int(Decimal(row["unit_price"]) * 100)
        if abs(quantity * (price_cents - costs[row["product_id"]])) > (1 << 63) - 1:
            raise ValueError(f"Profit for order {row['order_id']} and product {row['product_id']} exceeds the Java MapReduce integer range.")


def run_analysis(
    orders_source: str | Path | bytes | BinaryIO,
    products_source: str | Path | bytes | BinaryIO | None = None,
    *,
    hadoop: str | None = None,
    jar_path: str | Path | None = None,
    runs_dir: str | Path | None = None,
    hdfs_root: str | None = None,
    timeout_seconds: int | None = None,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    settings = load_settings()
    hadoop = hadoop or settings.hadoop_executable
    jar_path = Path(jar_path or settings.mapreduce_jar)
    runs_dir = runs_dir or settings.runs_directory
    hdfs_root = hdfs_root or settings.hdfs_root
    timeout_seconds = timeout_seconds or settings.timeout_seconds
    run_dir = Path(runs_dir) / run_id
    staging_dir = run_dir / "staging"
    staging_dir.mkdir(parents=True, exist_ok=False)
    hdfs_run = f"{hdfs_root.rstrip('/')}/{run_id}"
    result: dict[str, Any] = {
        "run_id": run_id,
        "status": "RUNNING",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "hdfs_paths": {
            "input": f"{hdfs_run}/input",
            "aggregation": f"{hdfs_run}/output/aggregation",
            "ranking": f"{hdfs_run}/output/ranking",
            "join": f"{hdfs_run}/output/join",
        },
        "jobs": [],
        "results": None,
    }
    LOGGER.info("Run started run_id=%s", run_id)
    try:
        orders_path = staging_dir / "orders.csv"
        orders_report = validate_orders(orders_source, orders_path)
        if orders_report.accepted_rows == 0:
            raise ValueError("No valid transaction rows remain after validation.")
        result["validation"] = {"orders": _report_dict(orders_report)}
        selected_orders = _apply_filters(orders_report.clean_rows, filters)
        if not selected_orders:
            raise ValueError("No accepted transaction rows match the selected filters.")
        product_report = validate_products(products_source) if products_source is not None else None
        _validate_profit_line_ranges(selected_orders, product_report)
        order_headers = list(orders_report.clean_rows[0])
        with orders_path.open("w", encoding="utf-8", newline="") as orders_file:
            writer = csv.DictWriter(orders_file, fieldnames=order_headers, lineterminator="\n")
            writer.writeheader()
            writer.writerows(selected_orders)
        result["selected_rows"] = len(selected_orders)
        result["filters"] = filters or {}

        if product_report is not None:
            product_path = staging_dir / "products.csv"
            result["validation"]["products"] = _report_dict(product_report)
        else:
            product_path = staging_dir / "products.csv"
        has_unit_cost, product_count = _write_products(product_report, orders_report, product_path)
        result["has_profit"] = has_unit_cost
        result["product_lookup_rows"] = product_count

        hdfs = HDFSClient(hadoop)
        orders_hdfs = hdfs.upload(orders_path, result["hdfs_paths"]["input"], "orders.csv")
        products_hdfs = hdfs.upload(product_path, result["hdfs_paths"]["input"], "products.csv")
        result["hdfs_paths"]["orders"] = orders_hdfs
        result["hdfs_paths"]["products"] = products_hdfs

        job_runner = HadoopJobRunner(hadoop, jar_path, timeout_seconds)
        job_specs = [
            ("Sales aggregation", ORDERS_MAIN, [orders_hdfs, result["hdfs_paths"]["aggregation"]], result["hdfs_paths"]["aggregation"]),
            ("Product ranking", RANKING_MAIN, [result["hdfs_paths"]["aggregation"], result["hdfs_paths"]["ranking"]], result["hdfs_paths"]["ranking"]),
            ("Product lookup join", JOIN_MAIN, [orders_hdfs, products_hdfs, result["hdfs_paths"]["join"]], result["hdfs_paths"]["join"]),
        ]
        for name, main_class, arguments, output_path in job_specs:
            execution: JobExecution = job_runner.run(name, main_class, arguments, output_path)
            result["jobs"].append(execution.to_dict())
            if execution.status != "SUCCEEDED":
                raise RuntimeError(execution.error or execution.stderr or f"{name} failed.")

        reader = OutputReader(hdfs)
        joins = reader.read_joins(result["hdfs_paths"]["join"])
        result["results"] = {
            "aggregates": reader.read_aggregates(result["hdfs_paths"]["aggregation"]),
            "rankings": reader.read_rankings(result["hdfs_paths"]["ranking"]),
            "joins": joins,
        }
        result["missing_product_matches"] = sum(join["match_status"] == "MISSING" for join in joins)
        result["status"] = "SUCCEEDED"
        LOGGER.info("Run succeeded run_id=%s selected_rows=%d", run_id, len(selected_orders))
    except Exception as error:
        result["status"] = "FAILED"
        result["error"] = str(error)
        LOGGER.error("Run failed run_id=%s error=%s", run_id, error)
    finally:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        _save_result(run_dir, result)

    if result["status"] != "SUCCEEDED":
        raise PipelineFailure(result.get("error", "Hadoop pipeline failed."), result)
    return result