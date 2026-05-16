# f1_pipeline dbt project

Layering:

- `staging`: clean, cast, rename, deduplicate raw OpenF1 tables.
- `intermediate`: reusable business logic and rollups.
- `marts`: dashboard-ready dimensions, facts, and aggregates.

Common commands:

```bash
dbt deps
dbt debug
dbt build
dbt build --selector high_volume
dbt build --selector dashboard
dbt docs generate
```

Raw source schema is controlled by `SNOWFLAKE_RAW_SCHEMA`.
Target schema is controlled by `SNOWFLAKE_SCHEMA`.
