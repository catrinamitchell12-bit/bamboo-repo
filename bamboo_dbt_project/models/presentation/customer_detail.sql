{{ config(
    schema='presentation',
    materialized='table',
    unique_key='customer_ref'
) }}

WITH dispursed_loans AS (
SELECT
    quote.customer_ref,
    quote.loan_type,
    quote.brand,
    count(application.loan_ref) as count_disbursed_loans
FROM {{ ref('dim_applications') }} application
JOIN {{ ref('fact_quote') }} quote
ON application.quote_ref = quote.quote_ref
where application.loan_ref is not null
and application.status = 'Disbursed'
group by 1,2,3
)
,

plata_new as (
SELECT 
    *
FROM dispursed_loans
WHERE brand = 'Plata'
and loan_type = 'New'
)

,

plata_topup AS (
SELECT
    *
FROM dispursed_loans
WHERE brand = 'Plata'
and loan_type = 'TopUp'
),

plata_repeat AS (
SELECT
    *
FROM dispursed_loans
WHERE brand = 'Plata'
and loan_type = 'Repeat'
),

bamboo_new as (
SELECT 
    *
FROM dispursed_loans
WHERE brand = 'Bamboo'
and loan_type = 'New'
),

bamboo_topup AS (
SELECT
    *
FROM dispursed_loans
WHERE brand = 'Bamboo'
and loan_type = 'TopUp'
),

bamboo_repeat AS (
SELECT
    *
FROM dispursed_loans
WHERE brand = 'Bamboo'
and loan_type = 'Repeat'
)

,application_counts AS (
SELECT
    quote.customer_ref,
    quote.brand,
    count(distinct application.application_ref) as total_applications
FROM {{ ref('dim_applications') }} application
JOIN {{ ref('fact_quote') }} quote
ON application.quote_ref = quote.quote_ref
WHERE application.application_ref is not null
group by 1,2
),

bamboo_application AS (
SELECT
    customer_ref,
    total_applications
FROM application_counts
WHERE brand = 'Bamboo'
),

plata_application AS (
SELECT
    customer_ref,
    total_applications
FROM application_counts
WHERE brand = 'Plata'
),

first_application AS (
SELECT
    quote.customer_ref,
    application.application_ref,
    quote.brand,
    application.event_timestamp
FROM {{ ref('dim_applications') }} application
JOIN {{ ref('fact_quote') }} quote
ON application.quote_ref = quote.quote_ref
WHERE application_ref is not null
group by 1,2,3,4
qualify row_number() over(partition by quote.customer_ref, quote.brand order by application.event_timestamp asc)=1
),

first_application_bamboo AS (
SELECT
    *
FROM first_application
where brand = 'Bamboo'
),

first_application_plata AS (
SELECT
    *
FROM first_application
where brand = 'Plata'
),

first_loan AS (
SELECT
    quote.customer_ref,
    application.loan_ref,
    quote.brand,
    application.event_timestamp
FROM {{ ref('dim_applications') }} application
JOIN {{ ref('fact_quote') }} quote
ON application.quote_ref = quote.quote_ref
WHERE loan_ref is not null
group by 1,2,3,4
qualify row_number() over(partition by quote.customer_ref, quote.brand order by application.event_timestamp asc)=1
),

first_loan_bamboo AS (
SELECT
    *
FROM first_loan
where brand = 'Bamboo'
),

first_loan_plata AS (
SELECT
    *
FROM first_loan
where brand = 'Plata'
)

select
    customer.customer_ref,
    customer.income,
    customer.mortgage,
    customer.bills,
    customer.postcode,
    customer.phone_number,
    plata_new.count_disbursed_loans as plata_total_new_loans,
    plata_topup.count_disbursed_loans as plata_total_topup_loans,
    plata_repeat.count_disbursed_loans as plata_total_repeat_loans,
    first_application_plata.application_ref as plata_first_application_ref,
    first_loan_plata.loan_ref as plata_first_loan_ref,
    bamboo_new.count_disbursed_loans as bamboo_total_new_loans,
    bamboo_topup.count_disbursed_loans as bamboo_total_topup_loans,
    bamboo_repeat.count_disbursed_loans as bamboo_total_repeat_loans,
    first_application_bamboo.application_ref as bamboo_first_application_ref,
    first_loan_bamboo.loan_ref as bamboo_first_loan_ref,
    plata_application.total_applications as plata_total_applications,
    bamboo_application.total_applications as bamboo_total_applications
from {{ ref('dim_customer') }} as customer
left join plata_new
on customer.customer_ref = plata_new.customer_ref
left join plata_topup
on customer.customer_ref = plata_topup.customer_ref
left join plata_repeat
on customer.customer_ref = plata_repeat.customer_ref
left join bamboo_new
on customer.customer_ref = bamboo_new.customer_ref
left join bamboo_repeat
on customer.customer_ref = bamboo_repeat.customer_ref
left join bamboo_topup
on customer.customer_ref = bamboo_topup.customer_ref
left join bamboo_application
on customer.customer_ref = bamboo_application.customer_ref
left join plata_application
on customer.customer_ref = plata_application.customer_ref
left join first_application_bamboo
on first_application_bamboo.customer_ref = customer.customer_ref
left join first_application_plata
on first_application_plata.customer_ref = customer.customer_ref
left join first_loan_bamboo
on first_loan_bamboo.customer_ref = customer.customer_ref
left join first_loan_plata
on first_loan_plata.customer_ref = customer.customer_ref