"""CSV validation and normalization for e-commerce input files."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import BinaryIO

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
MAX_LONG = (1 << 63) - 1
ORDER_COLUMNS = (
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "region",
    "category",
)
PRODUCT_COLUMNS = ("product_id", "product_name", "category")


@dataclass
class ValidationIssue:
    row_number: int
    message: str


@dataclass
class ValidationReport:
    total_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)
    clean_rows: list[dict[str, str]] = field(default_factory=list)
    headers: tuple[str, ...] = ()


def _read_text(source: str | Path | bytes | BinaryIO) -> str:
    if isinstance(source, bytes):
        raw = source
    elif isinstance(source, (str, Path)):
        source_path = Path(source)
        if source_path.stat().st_size > MAX_UPLOAD_BYTES:
            raise ValueError(f"Upload exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.")
        raw = source_path.read_bytes()
    else:
        raw = source.read()
        if isinstance(raw, str):
            if len(raw.encode("utf-8")) > MAX_UPLOAD_BYTES:
                raise ValueError(f"Upload exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.")
            return raw.lstrip("\ufeff")
    if len(raw) > MAX_UPLOAD_BYTES:
        raise ValueError(f"Upload exceeds the {MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("CSV must be encoded as UTF-8.") from error


def _reader(text: str) -> tuple[csv.DictReader, tuple[str, ...]]:
    stream = io.StringIO(text, newline="")
    reader = csv.DictReader(stream, strict=True)
    headers = tuple((header or "").strip() for header in (reader.fieldnames or ()))
    if not headers or any(not header for header in headers):
        raise ValueError("CSV must have a non-empty header row.")
    if len(set(headers)) != len(headers):
        raise ValueError("CSV contains duplicate column names.")
    reader.fieldnames = list(headers)
    return reader, headers


def _parse_date(value: str) -> str:
    value = value.strip()
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError:
        pass
    for pattern in ("%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    raise ValueError("order_date must be YYYY-MM-DD, MM/DD/YYYY, or DD-MM-YYYY")


def _decimal_text(value: str, field_name: str, *, allow_zero: bool = True) -> str:
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{field_name} must be numeric") from error
    if not amount.is_finite() or amount < 0 or (not allow_zero and amount == 0):
        qualifier = "positive" if not allow_zero else "non-negative"
        raise ValueError(f"{field_name} must be {qualifier}")
    try:
        rounded_amount = amount.quantize(Decimal("0.01"))
    except InvalidOperation as error:
        raise ValueError(f"{field_name} is too large to represent in cents") from error
    if amount != rounded_amount:
        raise ValueError(f"{field_name} may have at most two decimal places")
    amount = rounded_amount
    if amount * 100 > MAX_LONG:
        raise ValueError(f"{field_name} is too large for the Java MapReduce money representation")
    return format(amount, "f")


def _normalize_labels(row: dict[str, str | None]) -> dict[str, str]:
    return {key: (value or "").strip() for key, value in row.items()}


def validate_orders(
    source: str | Path | bytes | BinaryIO,
    output_path: str | Path | None = None,
) -> ValidationReport:
    """Validate transaction rows and optionally write a normalized CSV."""
    reader, headers = _reader(_read_text(source))
    missing_columns = sorted(set(ORDER_COLUMNS) - set(headers))
    if missing_columns:
        raise ValueError(f"orders.csv is missing required columns: {', '.join(missing_columns)}")
    has_unit_cost = "unit_cost" in headers
    output_headers = list(ORDER_COLUMNS) + (["unit_cost"] if has_unit_cost else [])
    report = ValidationReport(headers=headers)

    try:
        for row_number, raw_row in enumerate(reader, start=2):
            report.total_rows += 1
            if None in raw_row:
                report.rejected_rows += 1
                report.issues.append(ValidationIssue(row_number, "row has more fields than the header"))
                continue
            row = _normalize_labels(raw_row)
            errors: list[str] = []
            if any("\n" in value or "\r" in value for value in row.values()):
                errors.append("embedded line breaks are not supported in a field")
            if "\t" in row["product_id"]:
                errors.append("product_id must not contain tab characters")
            for column in ("order_id", "product_id", "order_date", "quantity", "unit_price"):
                if not row.get(column):
                    errors.append(f"{column} is required")
            if row.get("order_date"):
                try:
                    row["order_date"] = _parse_date(row["order_date"])
                except ValueError as error:
                    errors.append(str(error))
            if row.get("quantity"):
                quantity = row["quantity"]
                if not quantity.isascii() or not quantity.isdigit() or len(quantity) > 19 or int(quantity) <= 0:
                    errors.append("quantity must be a positive integer")
                elif int(quantity) > MAX_LONG:
                    errors.append("quantity exceeds the Java MapReduce integer range")
                else:
                    row["quantity"] = str(int(quantity))
            if row.get("unit_price"):
                try:
                    row["unit_price"] = _decimal_text(row["unit_price"], "unit_price")
                except ValueError as error:
                    errors.append(str(error))
            if row.get("quantity") and row.get("unit_price"):
                quantity = int(row["quantity"]) if row["quantity"].isdigit() else 0
                price_cents = int(Decimal(row["unit_price"]) * 100)
                if quantity * price_cents > MAX_LONG:
                    errors.append("line revenue exceeds the Java MapReduce integer range")
            if has_unit_cost and row.get("unit_cost"):
                try:
                    row["unit_cost"] = _decimal_text(row["unit_cost"], "unit_cost")
                except ValueError as error:
                    errors.append(str(error))
            if errors:
                report.rejected_rows += 1
                report.issues.append(ValidationIssue(row_number, "; ".join(errors)))
            else:
                report.accepted_rows += 1
                report.clean_rows.append({key: row.get(key, "") for key in output_headers})
    except csv.Error as error:
        raise ValueError(f"Malformed CSV near line {reader.line_num}: {error}") from error

    if output_path is not None:
        destination = Path(output_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=output_headers, lineterminator="\n")
            writer.writeheader()
            writer.writerows(report.clean_rows)
    return report


def validate_products(source: str | Path | bytes | BinaryIO) -> ValidationReport:
    """Validate the optional product lookup supplied to the join job."""
    reader, headers = _reader(_read_text(source))
    missing_columns = sorted(set(PRODUCT_COLUMNS) - set(headers))
    if missing_columns:
        raise ValueError(f"products.csv is missing required columns: {', '.join(missing_columns)}")
    report = ValidationReport(headers=headers)
    has_unit_cost = "unit_cost" in headers
    seen_product_ids: set[str] = set()
    try:
        for row_number, raw_row in enumerate(reader, start=2):
            report.total_rows += 1
            row = _normalize_labels(raw_row)
            errors = [f"{column} is required" for column in PRODUCT_COLUMNS if not row.get(column)]
            if any("\n" in value or "\r" in value for value in row.values()):
                errors.append("embedded line breaks are not supported in a field")
            if "\t" in row["product_id"]:
                errors.append("product_id must not contain tab characters")
            if row.get("product_id") in seen_product_ids:
                errors.append("product_id must be unique in the lookup")
            if row.get("product_id"):
                seen_product_ids.add(row["product_id"])
            if has_unit_cost:
                if not row.get("unit_cost"):
                    errors.append("unit_cost is required when the column is present")
                else:
                    try:
                        row["unit_cost"] = _decimal_text(row["unit_cost"], "unit_cost")
                    except ValueError as error:
                        errors.append(str(error))
            if errors or None in raw_row:
                report.rejected_rows += 1
                message = "; ".join(errors) or "row has more fields than the header"
                report.issues.append(ValidationIssue(row_number, message))
            else:
                report.accepted_rows += 1
                report.clean_rows.append(row)
    except csv.Error as error:
        raise ValueError(f"Malformed CSV near line {reader.line_num}: {error}") from error
    return report