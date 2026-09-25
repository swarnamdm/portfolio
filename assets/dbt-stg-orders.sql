{{ config(materialized='view') }}

/*
  dbt staging model: stg_orders
  Demo / concept model. Cleans the raw orders landing table:
  renames, casts types, drops invalid rows, adds a derived order_date.
*/

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_ts as timestamp)        as order_ts,
        cast(quantity  as integer)         as quantity,
        cast(unit_price as numeric(10, 2)) as unit_price,
        upper(trim(status))               as status,
        date(cast(order_ts as timestamp)) as order_date
    from source
    where order_id is not null
      and quantity > 0
)

select * from cleaned
