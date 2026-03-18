# Snowflake dbt Logistics Pipeline

A dbt project modelling a supply chain / logistics pipeline on top of Snowflake's
built-in **TPC-H sample dataset**. No external data loading required — just point
your profile at a Snowflake account and run.

---

## What this project does

Transforms raw TPC-H tables (orders, line items, suppliers, customers, parts, nations)
into a clean analytics layer with:

- **On-time delivery tracking** per line item, order, and supplier
- **Supplier performance tiers** (elite → at_risk) derived from on-time rates
- **Customer lifetime value** and value tiers (platinum → bronze)
- **Incremental loading** on the core shipment fact table
- **SCD snapshots** capturing supplier tier changes over time

---

## Project structure

```
models/
├── staging/          # views — rename, cast, derive simple fields
│   ├── sources.yml   # declares snowflake_sample_data.tpch_sf1
│   ├── schema.yml    # column tests + docs
│   ├── stg_orders.sql
│   ├── stg_line_items.sql
│   ├── stg_suppliers.sql
│   ├── stg_customers.sql
│   ├── stg_parts.sql
│   └── stg_nations.sql
│
├── intermediate/     # ephemeral — joins + business logic, no extra DB objects
│   ├── int_shipments_enriched.sql
│   └── int_supplier_performance.sql
│
└── marts/            # tables — analytics-ready, BI-tool friendly
    ├── schema.yml
    ├── fct/
    │   ├── fct_shipments.sql        # incremental, merge strategy
    │   └── fct_order_summary.sql
    └── dim/
        ├── dim_suppliers.sql
        └── dim_customers.sql

snapshots/
└── snap_supplier_performance_tier.sql   # tracks tier changes over time

tests/
├── assert_no_negative_revenue.sql
├── assert_on_time_rate_between_0_and_100.sql
└── assert_shipments_gt_orders.sql

macros/
├── cents_to_dollars.sql
├── safe_divide.sql
└── date_trunc.sql

analyses/
└── late_shipment_deep_dive.sql   # ad-hoc, not compiled to warehouse
```

---

## Data source

Uses **Snowflake Sample Data → TPC-H SF1** — available on every Snowflake account.

```
database : SNOWFLAKE_SAMPLE_DATA
schema   : TPCH_SF1
tables   : ORDERS, LINEITEM, SUPPLIER, CUSTOMER, PART, NATION, REGION
```

No seeds, no uploads. Just configure your profile and run.

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/your-username/snowflake-dbt-logistics.git
cd snowflake-dbt-logistics
```

### 2. Install dbt

```bash
pip install dbt-snowflake
```

### 3. Configure your profile

Add the following to `~/.dbt/profiles.yml`:

```yaml
logistics_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "GETISCJ-ZOB29800"
      user: "LHAKSON17"
      role: "ACCOUNTADMIN"
      database: "Lhakson_DB"  
      warehouse: "COMPUTE_WH" 
      schema: dev
      threads: 4
```

### 4. Run the project

```bash
# check connection
dbt debug

# run all models
dbt run

# run tests
dbt test

# run snapshots (schedule this weekly in production)
dbt snapshot

# compile analyses without running them
dbt compile
```

---

## Intermediate-level patterns used

| Pattern | Where |
|---|---|
| Source declarations with tests | `staging/sources.yml` |
| 3-layer architecture (staging → intermediate → marts) | `models/` |
| Ephemeral intermediate models | `intermediate/` |
| Incremental model with merge strategy | `fct_shipments.sql` |
| Window functions (RANK, running totals) | `int_supplier_performance.sql`, `fct_shipments.sql` |
| Column-level docs + schema tests | `schema.yml` files |
| Custom singular tests | `tests/` |
| Reusable Jinja macros | `macros/` |
| SCD Type 2 snapshot | `snap_supplier_performance_tier.sql` |
| Ad-hoc analysis layer | `analyses/` |

---

## Key business metrics

| Model | Grain | Key metrics |
|---|---|---|
| `fct_shipments` | order + line item | `is_on_time`, `days_vs_commit`, `net_revenue_usd`, `transit_days` |
| `fct_order_summary` | order | `is_fully_on_time`, `total_discount_usd`, `unique_suppliers` |
| `dim_suppliers` | supplier | `performance_tier`, `on_time_rate_pct`, `rank_in_region` |
| `dim_customers` | customer | `value_tier`, `lifetime_value_usd`, `avg_order_value_usd` |

---

## Next steps / potential extensions

- Add `packages.yml` with `dbt-utils` for `generate_surrogate_key` and `date_spine`
- Wire up to Airflow or dbt Cloud for scheduled runs
- Build a Tableau / Metabase dashboard on top of the marts layer
- Add `dbt-expectations` for advanced data quality checks