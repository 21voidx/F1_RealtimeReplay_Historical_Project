{{ config(materialized='table', tags=['dimension']) }}
select * from {{ ref('stg_openf1__drivers') }}
