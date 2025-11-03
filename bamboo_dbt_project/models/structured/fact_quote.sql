{{ config(
    schema='structured',
    materialized='incremental',
    unique_key='quote_ref'
) }}

SELECT
    quote_ref,
    customer_ref,
    loan_type,
    brand,
    loan_value,
    loan_term,
    event_timestamp,
    load_timestamp
FROM {{ ref('stg_loans') }}

{% if is_incremental() %}
  -- Only pull rows that are new or updated since the last run
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}