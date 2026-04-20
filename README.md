# UK Housing Market Pipeline

A data engineering pipeline that ingests UK government house price 
data, loads it into Snowflake, and transforms it using dbt to produce 
clean, analysis-ready models — revealing which counties and property 
types command the highest prices across England and Wales.

## Why This Exists

House price data is publicly available but raw and hard to query. 
This pipeline takes the UK government's Price Paid dataset, structures 
it properly, decodes cryptic single-letter codes into readable labels, 
and produces a mart model that answers real estate market questions 
in seconds.

## Pipeline Architecture

```
UK Gov CSV → ingest.py → S3 (raw sample)
                → load.py → Snowflake (raw_housing table)
                      → dbt stg_housing → clean view
                            → dbt housing_trends → market analysis
                                  → dbt test → data quality checks
```

## Tech Stack

- **Python** — file ingestion and S3/Snowflake loading
- **Pandas** — CSV reading and bulk loading via write_pandas
- **AWS S3** — raw data storage
- **Snowflake** — cloud data warehouse
- **dbt** — data transformation, modelling and testing
- **SQL** — analysis and mart queries

## What's New in This Project

This project introduces **dbt** — replacing the `clean.py` Python 
script from previous projects with SQL models that run directly 
inside Snowflake.

| Previous Projects | This Project |
|---|---|
| Python cleans data | dbt SQL models clean data |
| Manual transformation | Automated, version-controlled models |
| No data quality checks | dbt tests run automatically |
| No documentation | dbt generates docs from schema.yml |

## Project Structure

```
uk-housing-pipeline/
├── config/
│   └── config.py            # reads credentials from .env
├── scripts/
│   ├── ingest.py            # reads CSV, samples 10k rows, pushes to S3
│   └── load.py              # bulk loads S3 CSV into Snowflake
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_housing.sql     # cleaning layer
│   │   │   ├── schema.yml          # column definitions and tests
│   │   │   └── sources.yml         # source table reference
│   │   └── marts/
│   │       └── housing_trends.sql  # analysis layer
│   └── dbt_project.yml
├── .env                     # credentials (never committed)
├── .gitignore
└── README.md
```

## Setup

1. Clone the repo
2. Download the UK Price Paid dataset:
   https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
3. Install dependencies:
   ```bash
   pip install pandas boto3 snowflake-connector-python dbt-snowflake
   pip install "snowflake-connector-python[pandas]"
   ```
4. Create a `.env` file:
   ```
   AWS_ACCESS_KEY_ID=your_key
   AWS_SECRET_ACCESS_KEY=your_secret
   AWS_BUCKET_NAME=your_bucket
   AWS_REGION=your_region
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   ```
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
```
