/*
  One row per firm: its most recent stored version, with JSON fields parsed and typed.
  Field names are exactly those returned by the FCA Register API.
  extracted_at = when this version was first seen (raw stores changes only).
*/
with latest as (
    select *
    from {{ ref('raw_fca_firms') }}
    qualify row_number() over (partition by frn order by extracted_at desc, loaded_at desc) = 1
),

addresses as (
    select frn, unnest(cast(coalesce(addresses, '[]') as json[])) as addr
    from latest
),

principal_address as (
    -- prefer the "Principal Place of Business" address, else any address
    select frn, addr
    from addresses
    qualify row_number() over (
        partition by frn
        order by coalesce(addr ->> 'Address Type' ilike '%principal%', false) desc
    ) = 1
)

select
    l.frn,
    {{ clean_text("l.firm ->> 'Organisation Name'") }}                as firm_name,
    lower({{ clean_text("l.firm ->> 'Status'") }})                     as status,
    {{ parse_fca_date("l.firm ->> 'Status Effective Date'") }}       as status_effective_date,
    {{ clean_text("l.firm ->> 'Sub-Status'") }}                        as sub_status,
    {{ clean_text("l.firm ->> 'Business Type'") }}                     as business_type,
    {{ clean_text("l.firm ->> 'Companies House Number'") }}            as companies_house_number,
    {{ clean_text("l.firm ->> 'Client Money Permission'") }}           as client_money_permission,
    {{ clean_text("l.firm ->> 'PSD / EMD Status'") }}                  as psd_emd_status,
    {{ clean_text("l.firm ->> 'MLRs Status'") }}                       as mlrs_status,
    {{ clean_text("p.addr ->> 'Address Line 1'") }}                    as address_line_1,
    {{ clean_text("p.addr ->> 'Address Line 2'") }}                    as address_line_2,
    {{ clean_text("p.addr ->> 'Town'") }}                              as town,
    upper({{ clean_text("p.addr ->> 'Postcode'") }})                   as postcode,
    {{ clean_text("p.addr ->> 'Country'") }}                           as country,
    {{ clean_text("p.addr ->> 'Website Address'") }}                   as website,
    l.extracted_at,
    l.loaded_at
from latest l
left join principal_address p on p.frn = l.frn
