# Qafila gross margin

Submitted by: Ramaz Abuarab — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

## Design

Files (SQLite + supplier CSVs) → `raw` (DuckDB, all text, with file name) → `staging` (dbt views: clean types, trimmed SKUs, Riyadh dates) → `silver` (dbt: cost history) → `gold` (dbt: weekly margin) → Iceberg (pyiceberg, updated in place).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_supplier_prices` | staging | supplier file row | view |
| `stg_orders`, `stg_order_items`, `stg_products`, `stg_stores` | staging | order / order line / product / store | view |
| `int_supplier_prices_checked` | silver | supplier file row with a reject reason (or null) | table |
| `product_cost_history` | silver | SKU × cost version | table |
| `weekly_margin` | gold | iso_week × city × category | table |
| `price_quarantine` | quality | rejected supplier row | table |

**How the SCD2 history is built:** Rejected rows are removed first (unknown SKU is checked before invalid cost). Then duplicates of the same (SKU, valid_from) are resolved by keeping the row from the file whose name sorts last, so `W39_fix` replaces `W39`. A new version starts only when the cost differs from the previous row of that SKU (`lag`), so SKUs missing from a weekly file simply keep their previous cost. `valid_to` is the next version's `valid_from` (`lead`), empty for the current version.

**How each order line finds its cost:** Left join of delivered lines to `product_cost_history` on SKU where `valid_from <= order_date < valid_to` (empty `valid_to` = open). Lines with no match stay in `lines` and `revenue_sar` and are counted in `uncosted_lines`.

## Data problems I found

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Unknown SKUs (not sold by Qafila) | supplier files | 3 | quarantined as `unknown_sku` |
| Zero / negative / invalid cost | supplier files | 4 | quarantined as `invalid_cost` |
| Decimal commas and Arabic-Indic digits | W38 file | most of the 160 rows | translated digits, replaced `,` with `.` before casting |
| Column renamed `unit_cost` → `cost_sar` (+ `currency`) | W39 file | 160 | read each file with its own columns, `coalesce` into one cost |
| Trailing spaces in SKUs | products | 2 | `trim` on both sides before comparing |
| Late correction file | `W39_fix` | 4 SKU versions changed | last file name wins; Iceberg updated in place |
| Order lines with no valid cost on their date | orders | 20,626 of 82,404 | kept as uncosted (counted in lines and revenue, not in cost) |

## Tests

| Test | On | What it protects against |
|---|---|---|
| `not_null` | `product_cost_history.sku`, `unit_cost_sar`, `valid_from` | broken parsing producing empty keys or costs |
| `not_null` + `accepted_values` | `price_quarantine.reason` | reasons other than `unknown_sku` / `invalid_cost` |
| `not_null` | `weekly_margin.iso_week`, `lines` | empty grouping keys |
| `one_current_per_sku` (singular) | `product_cost_history` | a SKU with zero or several current versions |

8 tests in total, all passing (`dbt build` runs inside `run.py`).

## Re-runs and the correction

Running `run.py` twice on the same data prints `unchanged:` for all three tables, so no new Iceberg snapshot is written.

After adding the correction file (`make_data.py --add-correction`) and running again, `product_cost_history` went from 633 to 637 versions and the tables printed `updated:`.
Time travel on `gold.weekly_margin` (`time_travel.py`):

| Snapshot | margin_sar | cost_sar |
|---|---|---|
| `8072227234110946115` (before correction) | 801,016.88 | 1,654,199.98 |
| `4890776605277797453` (after correction) | 803,515.80 | 1,651,701.06 |

(PyIceberg's `overwrite` also writes one intermediate empty snapshot; it holds no data and does not affect the numbers.)
The same pipeline also ran cleanly on a second dataset (`--seed 11`).

## What I'd do next

With 100× more data, the full refresh of `raw` and the full table comparison in `publish()` would be the first things to slow down: I would load incrementally per file and compare by hash. With daily supplier files, I would make the history model incremental and add a freshness test.

## AI assistance

I used Claude (Anthropic) as an assistant to design the pipeline, write the SQL and Python, and debug errors. I ran every command myself, checked the outputs with `tools/check.py`, and wrote the final README from my own results.