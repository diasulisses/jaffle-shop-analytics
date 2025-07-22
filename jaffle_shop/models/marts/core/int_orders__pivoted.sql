{{
    config(
        materialized='incremental',
        incremental_strategy = 'insert_overwrite',
        unique_key= 'order_id',
        partition_by= {

            'field': 'payment_created_at',
            'data_type': 'date',
            'granularity': 'day'

        },
        on_schema_change = 'fail'
    )
}}

{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

with payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

pivoted as (
    select
        order_id,
        payment_created_at,
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then payment_amount else 0 end) as {{ payment_method }}_amount
        {%- if not loop.last -%}
        ,
        {%- endif %}
        {% endfor -%}
    from payments
    where payment_status = 'success'
    group by order_id, payment_created_at
)

select * from pivoted