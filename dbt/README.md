
### Setup Snowflake
```text
use role accountadmin;

-- Membuat Role khusus dbt
CREATE ROLE dbt_role;

-- Membuat Virtual Warehouse (compute engine)
CREATE WAREHOUSE dbt_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Membuat Database dan Schema
CREATE DATABASE dbt_db;
CREATE SCHEMA dbt_db.dbt_schema;

GRANT ALL ON DATABASE dbt_db TO ROLE dbt_role;
GRANT ALL ON SCHEMA dbt_db.dbt_schema TO ROLE dbt_role;
GRANT USAGE ON WAREHOUSE dbt_wh TO ROLE dbt_role;

-- save cost
use role accountadmin;
drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
```