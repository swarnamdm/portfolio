{{ config(materialized='table') }}

/*
  dbt mart model: mart_revenue
  Demo / concept model. Business-ready daily revenue aggregate
  built on top of the staging layer via ref().
*/

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_date,
    count(*)                                   as order_count,
    sum(quantity)                              as units_sold,
    round(sum(quantity * unit_price), 2)       as gross_revenue,
    round(avg(quantity * unit_price), 2)       as avg_order_value
from orders
group by order_date
order by order_date
