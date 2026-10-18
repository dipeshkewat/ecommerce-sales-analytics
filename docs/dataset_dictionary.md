# Dataset Dictionary

## Input Files

| File | Column | Type / rule | Meaning |
| --- | --- | --- | --- |
| orders.csv | order_id | non-empty text | Business order identifier; repeated across lines is allowed. |
| orders.csv | order_date | ISO date or accepted documented date format | Transaction date; normalized to `YYYY-MM-DD`. |
| orders.csv | customer_id | text, may be blank | Customer identifier; not used to derive metrics. |
| orders.csv | product_id | non-empty text without tab/newline | Product lookup and aggregation key. |
| orders.csv | quantity | positive ASCII integer | Units on this transaction line. |
| orders.csv | unit_price | non-negative decimal, <= 2 decimal places | Per-unit selling price. |
| orders.csv | region | text | Trimmed regional label. |
| orders.csv | category | text | Trimmed transaction category label. |
| products.csv | product_id | unique, non-empty text | Join key. |
| products.csv | product_name | non-empty text | Product display name. |
| products.csv | category | non-empty text | Lookup category. |
| products.csv | unit_cost | optional non-negative decimal | When included, required for every accepted product row and used for profit. |

Optional columns in `orders.csv` are ignored for analysis. Unknown columns in product lookup rows are ignored by the normalized join input. Rows with invalid required fields are reported by source row number and not staged.

## Hadoop Output Schemas

All outputs are tab-separated text without headers. Base64 URL-safe fields omit padding.

- Aggregation: `AGG, group_type, base64(group_key), quantity, revenue_cents, distinct_order_count, transaction_line_count, average_order_value_cents`.
- Ranking: `metric, rank, base64(product_id), quantity, revenue_cents`. Metrics are `REVENUE` and `QUANTITY`; ranks are global because the job uses one reducer. Ties sort by product ID ascending.
- Joined transaction: `JOIN, base64(product_id), base64(order_id), order_date, quantity, unit_price_cents, base64(product_name), base64(category), unit_cost_cents, profit_cents, MATCHED|MISSING`.
- Profit summary: `PROFIT, base64(product_id)|ALL, profit_cents, transaction_line_count`.

Money values are integer cents. Blank cost/profit fields indicate no cost was supplied for the joined product.