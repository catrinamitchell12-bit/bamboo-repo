{{ config(
    schema='structured',
    materialized='incremental',
    unique_key='quote_ref'
) }}
 
SELECT
    quote_ref,
    application_ref,
    loan_ref,
    previous_loan_ref,
    income_check_id,
    income_check,
    mortgage_check_id,
    mortgage_check,
    bills_check_id,
    bills_check,
    status,
    event_timestamp,
    load_timestamp
FROM {{ ref('stg_loans') }}
where application_ref is not null

{% if is_incremental() %}
  -- Only pull rows that are new or updated since the last run
  AND load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}