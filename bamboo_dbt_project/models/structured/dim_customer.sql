{{ config(
    schema='structured',
    materialized='incremental',
    unique_key='customer_ref'
) }}

WITH ordered_customers AS (
    SELECT
        customer_ref,
        income,
        mortgage,
        bills,
        postcode,
        phone_number,
        event_timestamp,
        load_timestamp,
        row_number() OVER (PARTITION BY customer_ref ORDER BY event_timestamp DESC) AS rn
    FROM {{ ref('stg_loans') }}
)
SELECT
    customer_ref,
    income,
    mortgage,
    bills,
    postcode,
    phone_number,
    event_timestamp,
    load_timestamp
FROM ordered_customers
WHERE rn = 1

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
    and load_timestamp >= (select coalesce(max(load_timestamp),'1900-01-01') from {{ this }})
{% endif %}