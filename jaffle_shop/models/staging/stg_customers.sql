select
    id as customer_id,
    split(name, ' ')[safe_offset(0)] as first_name,
    split(name, ' ')[safe_offset(1)] as last_name

from {{ source('main', 'raw_customers') }}