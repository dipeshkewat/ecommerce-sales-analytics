from __future__ import annotations

import csv
import io

import pytest

from src.ingestion.validation import validate_orders, validate_products

ORDER_HEADER = "order_id,order_date,customer_id,product_id,quantity,unit_price,region,category\n"


def test_orders_accept_quotes_commas_and_normalize_values(tmp_path):
    content = (
        ORDER_HEADER
        + ' O-1 ,01/02/2025,C-1,P-1,2,10.25, North ,"Home, Kitchen"\n'
        + 'O-2,2025-01-03,C-2,P-2,1,0,South,Toys\n'
    )
    output = tmp_path / "clean.csv"

    report = validate_orders(content.encode(), output)

    assert (report.total_rows, report.accepted_rows, report.rejected_rows) == (2, 2, 0)
    assert report.clean_rows[0]["order_date"] == "2025-01-02"
    assert report.clean_rows[0]["category"] == "Home, Kitchen"
    with output.open(encoding="utf-8", newline="") as clean_file:
        rows = list(csv.DictReader(clean_file))
    assert rows[0]["unit_price"] == "10.25"


def test_invalid_rows_are_reported_and_not_staged():
    content = ORDER_HEADER + "O-1,not-a-date,C-1,P-1,0,-2,North,Toys\n"

    report = validate_orders(content.encode())

    assert (report.total_rows, report.accepted_rows, report.rejected_rows) == (1, 0, 1)
    assert "positive integer" in report.issues[0].message
    assert "non-negative" in report.issues[0].message
    assert "order_date" in report.issues[0].message


def test_missing_columns_and_malformed_csv_fail_actionably():
    with pytest.raises(ValueError, match="missing required columns"):
        validate_orders(b"order_id,product_id\n")

    malformed = ORDER_HEADER + 'O-1,2025-01-01,C-1,P-1,1,1,North,"unfinished\n'
    with pytest.raises(ValueError, match="Malformed CSV"):
        validate_orders(malformed.encode())


def test_product_lookup_validates_cost_when_present():
    report = validate_products(
        b'product_id,product_name,category,unit_cost\nP-1,"Mug, steel",Home,3.25\nP-2,Widget,Toys,-1\n'
    )

    assert (report.total_rows, report.accepted_rows, report.rejected_rows) == (2, 1, 1)
    assert report.clean_rows[0]["product_name"] == "Mug, steel"
    assert "non-negative" in report.issues[0].message


def test_too_many_csv_fields_are_rejected():
    report = validate_orders((ORDER_HEADER + "O-1,2025-01-01,C-1,P-1,1,2,North,Toys,extra\n").encode())

    assert report.rejected_rows == 1
    assert "more fields" in report.issues[0].message


def test_java_long_cents_and_precision_are_enforced():
    content = (
        ORDER_HEADER
        + "O-1,2025-01-01,C-1,P-1,1,1.001,North,Toys\n"
        + "O-2,2025-01-01,C-1,P-1,9223372036854775807,2.00,North,Toys\n"
        + "O-3,2025-01-01,C-1,P-1,1,92233720368547758.08,North,Toys\n"
    )

    report = validate_orders(content.encode())

    assert (report.total_rows, report.accepted_rows, report.rejected_rows) == (3, 0, 3)
    assert "at most two decimal places" in report.issues[0].message
    assert "range" in report.issues[1].message
    assert "Java MapReduce" in report.issues[2].message


def test_products_with_unit_cost_require_a_valid_cost_on_each_row():
    report = validate_products(
        b"product_id,product_name,category,unit_cost\nP-1,Mug,Home,\nP-2,Widget,Toys,not-money\n"
    )

    assert report.accepted_rows == 0
    assert report.rejected_rows == 2
    assert "required when the column is present" in report.issues[0].message