with

orders as (
  select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
  select * from {{ ref('stg_stripe__payments') }}
),


order_amount as (
    select 
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1
),

paid_orders as (
    select orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        order_amount.total_amount_paid,
        order_amount.payment_finalized_date,
    from orders
    left join order_amount on orders.order_id = order_amount.order_id
)

select * from paid_orders