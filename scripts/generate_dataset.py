"""Generate labelled synthetic e-commerce transaction and product CSV files."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

PRODUCTS = (
    ("P001", "Travel Mug, Steel", "Home & Kitchen", "8.25", "14.99"),
    ("P002", 'Kids "Explorer" Set', "Toys", "12.00", "24.50"),
    ("P003", "Wireless Mouse", "Electronics", "9.80", "19.99"),
    ("P004", "Canvas Tote", "Accessories", "3.40", "11.00"),
    ("P005", "Desk Lamp", "Home & Kitchen", "15.00", "32.00"),
    ("P006", "Running Socks", "Sports", "2.10", "8.99"),
    ("P007", "Notebook Set", "Stationery", "4.25", "12.50"),
    ("P008", "Water Bottle", "Sports", "6.00", "16.75"),
    ("P009", "USB-C Hub", "Electronics", "18.50", "39.95"),
    ("P010", "Ceramic Planter", "Home & Kitchen", "7.20", "18.00"),
    ("P011", "Phone Stand", "Accessories", "2.60", "9.49"),
    ("P012", "Board Game", "Toys", "14.00", "29.99"),
)
REGIONS = ("North", "South", "East", "West", "Central")
ORDER_FIELDS = (
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "region",
    "category",
)


def generate_dataset(output_dir: Path, rows: int, seed: int = 2026) -> tuple[Path, Path]:
    if rows < 1:
        raise ValueError("rows must be at least 1")
    output_dir.mkdir(parents=True, exist_ok=True)
    randomizer = random.Random(seed)
    products_path = output_dir / "products.csv"
    orders_path = output_dir / "orders.csv"

    with products_path.open("w", encoding="utf-8", newline="") as products_file:
        writer = csv.writer(products_file, lineterminator="\n")
        writer.writerow(("product_id", "product_name", "category", "unit_cost"))
        for product_id, name, category, cost, _ in PRODUCTS:
            writer.writerow((product_id, name, category, cost))

    start_date = date(2024, 1, 1)
    with orders_path.open("w", encoding="utf-8", newline="") as orders_file:
        writer = csv.DictWriter(orders_file, fieldnames=ORDER_FIELDS, lineterminator="\n")
        writer.writeheader()
        line_number = 0
        order_number = 0
        while line_number < rows:
            order_number += 1
            order_date = (start_date + timedelta(days=randomizer.randrange(730))).isoformat()
            customer_id = f"C-{randomizer.randrange(1, 151):04d}"
            region = randomizer.choice(REGIONS)
            for _ in range(min(3, rows - line_number)):
                product_id, _, category, _, unit_price = randomizer.choice(PRODUCTS)
                writer.writerow(
                    {
                        "order_id": f"ORD-{order_number:06d}",
                        "order_date": order_date,
                        "customer_id": customer_id,
                        "product_id": product_id,
                        "quantity": randomizer.randrange(1, 6),
                        "unit_price": unit_price,
                        "region": region,
                        "category": category,
                    }
                )
                line_number += 1
    return orders_path, products_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=300, help="transaction lines to generate")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated"))
    args = parser.parse_args()
    orders_path, products_path = generate_dataset(args.output_dir, args.rows, args.seed)
    print(f"Generated {args.rows} synthetic transaction lines: {orders_path}")
    print(f"Generated {len(PRODUCTS)} synthetic products: {products_path}")


if __name__ == "__main__":
    main()