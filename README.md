5. Configure dbt profile:
```bash
   cd dbt
   dbt debug
```

## Running the Pipeline

From the project root:

```bash
# Step 1 - Ingest
PYTHONPATH=. python scripts/ingest.py

# Step 2 - Load
PYTHONPATH=. python scripts/load.py

# Step 3 - Transform with dbt (run from dbt/ folder)
cd dbt
dbt run --select stg_housing
dbt run --select housing_trends

# Step 4 - Test
dbt test
```

## Sample Output

Top counties by average sale price (2024):

| County | Total Sales | Avg Price |
|---|---|---|
| Surrey | 48 | £1,023,481 |
| Worcestershire | 5 | £1,005,000 |
| Cornwall | 2 | £732,500 |
| Windsor and Maidenhead | 99 | £610,258 |
| Greater London | 1,989 | £591,371 |
| Buckinghamshire | 283 | £571,845 |
| West Sussex | 13 | £553,577 |
| Oxfordshire | 670 | £495,509 |

## dbt Models

**Staging — `stg_housing`**
Reads from `raw_housing` and produces a clean view with:
- Single-letter property type codes decoded (T → Terraced, D → Detached etc.)
- Old/new flag decoded (Y → New Build, N → Existing)
- Duration decoded (F → Freehold, L → Leasehold)
- Date strings cast to proper DATE type
- Null locality and saon fields replaced with empty strings
- Amendment records filtered out

**Mart — `housing_trends`**
Reads from `stg_housing` and aggregates:
- Average, min and max sale price by county
- Total transaction count per county
- Ordered by average price descending

## dbt Tests

Five automated tests defined in `schema.yml`:

| Test | Column | Result |
|---|---|---|
| not_null | transaction_id | ✅ Pass |
| unique | transaction_id | ❌ 2,629 duplicates found |
| not_null | price | ✅ Pass |
| not_null | county | ✅ Pass |
| not_null | property_type | ✅ Pass |
| accepted_values | property_type | ✅ Pass |

The duplicate transaction IDs are a known characteristic of the UK 
Price Paid dataset — the government publishes amendment records with 
the same transaction ID when sale prices are corrected after 
completion. The test correctly identified this data quality issue 
in the source.