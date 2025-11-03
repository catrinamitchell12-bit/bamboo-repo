{{ config(
    schema='staging',
    materialized='incremental',
    unique_key='quote_ref'
) }}

SELECT
    "QuoteRef"::VARCHAR AS quote_ref,
    "CustomerRef"::VARCHAR AS customer_ref,
    nullif("Application Ref",'Null')::VARCHAR AS application_ref,
    nullif("Loan Ref",'Null')::VARCHAR AS loan_ref,
    "type"::VARCHAR AS loan_type,
    nullif("previous loan ref",'Null')::VARCHAR AS previous_loan_ref,
    "income"::INT AS income,
    "mortgage"::INT AS mortgage,
    "bills"::INT AS bills,
    "postcode"::VARCHAR AS postcode,
    "phone number"::VARCHAR AS phone_number,
    "Brand"::VARCHAR AS brand,
    "Value"::INT AS loan_value,
    "Term"::INT AS loan_term,
    nullif("Income_Check_id",'Null')::INT AS income_check_id,
    nullif("Income_Check", 'Null')::BOOLEAN AS income_check,
    nullif("Mortgage_check_id",'Null')::INT AS mortgage_check_id,
    nullif("Mortgage_check", 'Null')::BOOLEAN AS mortgage_check,
    nullif("Bills_Check_id",'Null')::INT AS bills_check_id,
    nullif("Bills_Check", 'Null')::BOOLEAN AS bills_check,
    "Status"::VARCHAR AS status,
    strptime("Time", '%d/%m/%Y %H:%M') AS event_timestamp,
    "load_timestamp"::TIMESTAMP AS load_timestamp

FROM {{ source('raw', 'prd_customer_daily') }}
WHERE quote_ref is not null

{% if is_incremental() %}
  -- Only pull rows that are new or updated since the last run
  AND strptime("Time", '%d/%m/%Y %H:%M') > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}
