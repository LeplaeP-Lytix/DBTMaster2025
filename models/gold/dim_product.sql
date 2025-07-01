with

order_items as (

    select * from {{ ref('stg_order_items') }}

),


orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('snapshot_products') }}

),

supplies as (

    select * from {{ ref('snapshot_supplies') }}

),

order_supplies_summary as (

    select
        product_id,
        dbt_scd_id,
        dbt_valid_from,
        dbt_valid_to,
        sum(supply_cost) as supply_cost
    from supplies
    group by dbt_scd_id, product_id, dbt_valid_from, dbt_valid_to

),

joined as (

    select
        order_items.*,

        orders.ordered_at,

        products.product_name,
        products.product_price,
        products.is_food_item,
        products.is_drink_item,

        order_supplies_summary.supply_cost

    from order_items

    left join orders on order_items.order_id = orders.order_id

    left join products on order_items.product_id = products.product_id
                        and orders.ordered_at < coalesce(products.dbt_valid_to, '2099-12-31')
                        and case 
                        when orders.ordered_at < (select min(products.dbt_valid_from) from products) then (select min(products.dbt_valid_from) from products)
                        else orders.ordered_at
                        end >= products.dbt_valid_from

    left join order_supplies_summary
        on order_items.product_id = order_supplies_summary.product_id
        and orders.ordered_at < coalesce(order_supplies_summary.dbt_valid_to, '2099-12-31')
        and case 
        when orders.ordered_at < (select min(order_supplies_summary.dbt_valid_from) from order_supplies_summary) then (select min(order_supplies_summary.dbt_valid_from) from order_supplies_summary)
        else orders.ordered_at
        end >= order_supplies_summary.dbt_valid_from

)

select * from joined