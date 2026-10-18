from __future__ import annotations

import base64

from src.hadoop.output_reader import OutputReader


class StubHDFS:
    def __init__(self, lines):
        self.lines = lines

    def read_part_files(self, path):
        return self.lines


def encoded(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


def test_output_reader_parses_aggregation_and_global_rank_rows():
    reader = OutputReader(
        StubHDFS(
            [
                f"AGG\tTOTAL\t{encoded('ALL')}\t3\t500\t1\t2\t500",
                f"AGG\tPRODUCT\t{encoded('P-1')}\t3\t500\t1\t2\t500",
            ]
        )
    )

    rows = reader.read_aggregates("/some/run/output")

    assert rows["total"][0]["revenue_cents"] == 500
    assert rows["product"][0]["order_count"] == 1
    assert rows["total"][0]["average_order_value_cents"] == 500


def test_output_reader_surfaces_missing_join_matches_and_java_profit_totals():
    reader = OutputReader(
        StubHDFS(
            [
                f"JOIN\t{encoded('P-9')}\t{encoded('O-1')}\t2025-01-01\t1\t500\t\t\t\t\tMISSING",
                f"JOIN\t{encoded('P-1')}\t{encoded('O-2')}\t2025-01-02\t2\t800\t{encoded('Mug')}\t{encoded('Home')}\t300\t1000\tMATCHED",
                f"PROFIT\t{encoded('P-1')}\t1000\t1",
                "PROFIT\tALL\t1000\t1",
            ]
        )
    )

    result = reader.read_joins("/some/run/output")

    assert result["rows"][0]["match_status"] == "MISSING"
    assert result["rows"][1]["product_name"] == "Mug"
    assert result["profit"][-1]["profit_cents"] == 1000