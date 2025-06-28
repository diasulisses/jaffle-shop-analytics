select 
    customer_id,
    first_name,
    last_name,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(order_id) as total_orders,
    sum(order_total) as total_spent
from {{ ref('stg_orders') }}
left join {{ ref('stg_customers') }} using (customer_id)
group by customer_id, first_name, last_name