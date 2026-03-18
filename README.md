# Snowflake dbt Logistics Pipeline

A dbt project modelling a supply chain pipeline on top of Snowflake built-in TPC-H sample dataset. No external data loading required.

## Data Source
Uses Snowflake Sample Data TPC-H SF1 (available on every Snowflake account).
- Database: SNOWFLAKE_SAMPLE_DATA
- Schema: TPCH_SF1
- Tables: ORDERS, LINEITEM, SUPPLIER, CUSTOMER, PART, NATION, REGION

## Project Structure
models/
  staging/        - views: rename, cast, derive simple fields
  intermediate/   - ephemeral: joins and business logic
  marts/fct/      - incremental fact tables
  marts/dim/      - dimension tables

## Models
| Model | Grain | Key metrics |
|---|---|---|
| fct_shipments | order + line item | is_on_time, days_vs_commit, net_revenue_usd |
| fct_order_summary | order | is_fully_on_time, total_discount_usd |
| dim_suppliers | supplier | performance_tier, on_time_rate_pct, rank_in_region |
| dim_customers | customer | value_tier, lifetime_value_usd |

## Intermediate Patterns Used
- Source declarations in sources.yml
- 3-layer architecture (staging, intermediate, marts)
- Ephemeral intermediate models
- Incremental model with merge strategy on fct_shipments
- Window functions: RANK, running totals
- Column-level schema tests
- Custom singular tests
- Reusable Jinja macros

## Setup
1. Install dbt: pip install dbt-snowflake
2. Configure ~/.dbt/profiles.yml with your Snowflake credentials
3. Run: dbt debug, dbt run, dbt test
