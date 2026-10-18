from __future__ import annotations

import csv

from scripts.generate_dataset import generate_dataset


def test_generator_is_deterministic_and_keeps_order_attributes_consistent(tmp_path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_orders, _ = generate_dataset(first_dir, rows=101, seed=42)
    second_orders, _ = generate_dataset(second_dir, rows=101, seed=42)

    assert first_orders.read_bytes() == second_orders.read_bytes()
    with first_orders.open(encoding="utf-8", newline="") as orders_file:
        rows = list(csv.DictReader(orders_file))
    assert len(rows) == 101
    by_order = {}
    for row in rows:
        attributes = (row["order_date"], row["customer_id"], row["region"])
        assert by_order.setdefault(row["order_id"], attributes) == attributes