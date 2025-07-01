with 
customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders_summary as (
    select  
        orders.customer_id,
        count(distinct orders.order_id) as count_lifetime_orders,
        case 
        when count(distinct orders.order_id) > 1 then 'returning'
        else 'new' 
        END as customer_type,
        min(orders.ordered_at) as first_ordered_at,
        max(orders.ordered_at) as last_ordered_at,
        sum(orders.subtotal) as lifetime_spend_pretax,
        sum(orders.tax_paid) as lifetime_tax_paid,
        sum(orders.order_total) as lifetime_spend
    from orders
    group by customer_id
),

joined as (
    select
        customers.*,
        customer_orders_summary.count_lifetime_orders,
        customer_orders_summary.first_ordered_at,
        customer_orders_summary.last_ordered_at,
        customer_orders_summary.lifetime_spend_pretax,
        customer_orders_summary.lifetime_tax_paid,
        customer_orders_summary.lifetime_spend,
        customer_orders_summary.customer_type
    from customers
    left join customer_orders_summary
    on customers.customer_id = customer_orders_summary.customer_id
)

select * from joined