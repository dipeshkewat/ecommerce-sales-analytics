"""Parse documented tab-separated outputs written by the Java jobs."""

from __future__ import annotations

import base64
from collections import defaultdict
from typing import Any

from src.hadoop.hdfs_client import HDFSClient


def _decode(value: str) -> str:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode("utf-8")


class OutputReader:
    def __init__(self, hdfs: HDFSClient):
        self.hdfs = hdfs

    def read_aggregates(self, hdfs_directory: str) -> dict[str, list[dict[str, Any]]]:
        tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for line in self.hdfs.read_part_files(hdfs_directory):
            columns = line.split("\t")
            if len(columns) != 8 or columns[0] != "AGG":
                raise ValueError(f"Unexpected aggregation output row: {line!r}")
            _, group_type, encoded_key, quantity, revenue_cents, order_count, line_count, average_order_value_cents = columns
            tables[group_type.lower()].append(
                {
                    "key": _decode(encoded_key),
                    "quantity": int(quantity),
                    "revenue_cents": int(revenue_cents),
                    "order_count": int(order_count),
                    "line_count": int(line_count),
                    "average_order_value_cents": int(average_order_value_cents),
                }
            )
        return dict(tables)

    def read_rankings(self, hdfs_directory: str) -> dict[str, list[dict[str, Any]]]:
        rankings: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for line in self.hdfs.read_part_files(hdfs_directory):
            columns = line.split("\t")
            if len(columns) != 5:
                raise ValueError(f"Unexpected ranking output row: {line!r}")
            metric, rank, encoded_product, quantity, revenue_cents = columns
            rankings[metric.lower()].append(
                {
                    "rank": int(rank),
                    "product_id": _decode(encoded_product),
                    "quantity": int(quantity),
                    "revenue_cents": int(revenue_cents),
                }
            )
        return dict(rankings)

    def read_joins(self, hdfs_directory: str) -> dict[str, list[dict[str, Any]]]:
        joins = []
        profits = []
        for line in self.hdfs.read_part_files(hdfs_directory):
            columns = line.split("\t")
            if columns[0] == "PROFIT" and len(columns) == 4:
                profits.append(
                    {
                        "product_id": "ALL" if columns[1] == "ALL" else _decode(columns[1]),
                        "profit_cents": int(columns[2]),
                        "line_count": int(columns[3]),
                    }
                )
                continue
            if len(columns) != 11 or columns[0] != "JOIN":
                raise ValueError(f"Unexpected product join output row: {line!r}")
            (
                _, encoded_product, encoded_order, order_date, quantity, unit_price_cents,
                encoded_name, encoded_category, unit_cost_cents, profit_cents, match_status,
            ) = columns
            joins.append(
                {
                    "product_id": _decode(encoded_product),
                    "order_id": _decode(encoded_order),
                    "order_date": order_date,
                    "quantity": int(quantity),
                    "unit_price_cents": int(unit_price_cents),
                    "product_name": _decode(encoded_name) if encoded_name else "",
                    "category": _decode(encoded_category) if encoded_category else "",
                    "unit_cost_cents": int(unit_cost_cents) if unit_cost_cents else None,
                    "profit_cents": int(profit_cents) if profit_cents else None,
                    "match_status": match_status,
                }
            )
        return {"rows": joins, "profit": profits}