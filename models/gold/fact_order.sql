with 

orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items_enr') }}
),

stg_locations as (
    select * from {{ ref('stg_locations') }}
),

fact as (
    select 
        orders.order_id,
        orders.location_id,
        orders.customer_id,
        order_items.product_id,
        order_items.product_price,
        (order_items.product_price * stg_locations.tax_rate) as product_tax,
        (order_items.product_price + (order_items.product_price * stg_locations.tax_rate)) as product_total 
    from orders
    left join order_items
    on orders.order_id = order_items.order_id
    left join stg_locations
    on orders.location_id = stg_locations.location_id
)

select * from fact