{{ config(
    schema='presentation',
    materialized='table',
    unique_key='quote_ref'
) }}

WITH deduplicated_previous_loan as (
select
    *
from {{ ref('dim_applications') }}
qualify row_number() over(partition by application_ref order by event_timestamp asc) = 1
)

SELECT
    quote.quote_ref,
    application.application_ref,
    quote.customer_ref,
    quote.brand,
    quote.loan_type,
    application.status,
    application.previous_loan_ref,
    first_value(application.previous_loan_ref) over(partition by application.application_ref order by application.event_timestamp asc) as first_loan_ref_in_series,
    application.event_timestamp
FROM deduplicated_previous_loan application
JOIN {{ ref('fact_quote') }} quote
ON application.quote_ref = quote.quote_ref
