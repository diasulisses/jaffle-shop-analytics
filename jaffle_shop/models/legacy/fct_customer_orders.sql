with 

-- Import CTEs

customers as (
  select * from {{ source('jaffle_shop', 'customers') }}
),

orders as (
  select * from {{ source('jaffle_shop', 'orders') }}
),

payments as (
  select * from {{ source('stripe', 'payments') }}
),

-- Logical CTE
order_amount as (
    select 
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
),

paid_orders as (
    select orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        order_amount.total_amount_paid,
        order_amount.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join order_amount on orders.id = order_amount.order_id
    left join customers on orders.user_id = customers.id
),

ltv as (
    select
        paid_orders.order_id,
        sum(total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_id
            rows between unbounded preceding and current row
        ) as clv
    from paid_orders
    order by paid_orders.order_id
),

-- Final CTE
final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
        case
            when (
                rank() over (
                    partition by customer_id
                    order by paid_orders.order_placed_at, paid_orders.order_id
                ) = 1
            ) then 'new'
            else 'return' end as nsvr,
        ltv.clv as customer_lifetime_value,
        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_id
        ) as fdos
    from paid_orders
    left outer join ltv on ltv.order_id = paid_orders.order_id
    order by order_id
)

-- Simple Select Statment
select * from final
order by order_id

