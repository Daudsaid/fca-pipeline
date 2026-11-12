/*
  One row per firm: its most recent stored version, with JSON fields parsed,
  typed and cleaned. Field names are exactly those returned by the FCA Register API.
  extracted_at = when this version was first seen (raw stores changes only).

  Cleaning rules (see the data-quality notes in the README):
    - whitespace collapsed; placeholders ('NA', '-', 'N/A', '- -') become NULL
    - 01/01/1901 (the FCA's "unknown date" placeholder) becomes NULL;
      dates before 1980 or in the future are kept but flagged as suspect
    - town falls back to Address Line 4 / 3 when the FCA left Town blank
      (note the API's own typo: the key is "Address LIne 3"), then is title-cased
    - postcodes are upper-cased and stripped of punctuation; UK ones are validated
*/
{% set uk_postcode = "[A-Z]{1,2}[0-9][0-9A-Z]? ?[0-9][A-Z]{2}" %}
{% set left_register = "('no longer authorised', 'revoked', 'cancelled - authorised emi',
                         'cancelled - small emi', 'no longer registered as an appointed representative')" %}

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
),

parsed as (
    select
        l.frn,
        {{ clean_text("l.firm ->> 'Organisation Name'") }}                  as firm_name,
        lower({{ clean_text("l.firm ->> 'Status'") }})                       as status,
        {{ parse_fca_date("l.firm ->> 'Status Effective Date'") }}           as status_effective_date,
        {{ clean_text("l.firm ->> 'Sub-Status'") }}                          as sub_status,
        {{ clean_text("l.firm ->> 'Business Type'") }}                       as business_type,
        {{ clean_text("l.firm ->> 'Companies House Number'") }}              as companies_house_number,
        {{ clean_text("l.firm ->> 'Client Money Permission'") }}             as client_money_permission,
        {{ clean_text("l.firm ->> 'PSD / EMD Status'") }}                    as psd_emd_status,
        {{ clean_text("l.firm ->> 'MLRs Status'") }}                         as mlrs_status,
        {{ clean_text("p.addr ->> 'Address Line 1'") }}                      as address_line_1,
        {{ clean_text("p.addr ->> 'Address Line 2'") }}                      as address_line_2,
        coalesce({{ clean_text("p.addr ->> 'Address Line 3'") }},
                 {{ clean_text("p.addr ->> 'Address LIne 3'") }})            as address_line_3,
        {{ clean_text("p.addr ->> 'Address Line 4'") }}                      as address_line_4,
        {{ clean_text("p.addr ->> 'Town'") }}                                as town_raw,
        upper(trim(regexp_replace({{ clean_text("p.addr ->> 'Postcode'") }}, '[^A-Za-z0-9 ]', '', 'g')))
                                                                             as postcode_raw,
        {{ clean_text("p.addr ->> 'Country'") }}                             as country,
        {{ clean_text("p.addr ->> 'Website Address'") }}                     as website,
        l.extracted_at,
        l.loaded_at
    from latest l
    left join principal_address p on p.frn = l.frn
),

-- a fallback line is only a plausible town if it isn't a number/postcode, the country,
-- or a street/building ("Wheler Road", "Unit 4", "Kings House")
town_candidates as (
    select
        *,
        case
            when address_line_4 is not null
             and not regexp_matches(address_line_4, '[0-9]')
             and not regexp_matches(address_line_4, '(?i)\b(road|rd|street|st|lane|ln|avenue|ave|way|drive|dr|close|court|ct|house|building|floor|suite|unit|place|square|sq|terrace|crescent|row|walk|mews|wharf road|business park|industrial estate|po box)\b')
             and upper(address_line_4) is distinct from upper(country)
            then address_line_4
        end as line4_town,
        case
            when address_line_3 is not null
             and not regexp_matches(address_line_3, '[0-9]')
             and not regexp_matches(address_line_3, '(?i)\b(road|rd|street|st|lane|ln|avenue|ave|way|drive|dr|close|court|ct|house|building|floor|suite|unit|place|square|sq|terrace|crescent|row|walk|mews|wharf road|business park|industrial estate|po box)\b')
             and upper(address_line_3) is distinct from upper(country)
            then address_line_3
        end as line3_town
    from parsed
)

select
    frn,
    firm_name,
    status,
    case
        when status in ('authorised', 'registered', 'eea authorised', 'authorised - applied to cancel')
          or status like 'appointed representative%'   then 'On the Register'
        when status in {{ left_register }}             then 'Left the Register'
        when status = 'see full details'
         and business_type = 'PSD'                     then 'PSD agent (see details)'
        else 'Other'
    end                                                                      as status_group,
    status_effective_date,
    coalesce(status_effective_date < date '1980-01-01'
             or status_effective_date > current_date, false)                 as date_suspect,
    sub_status,
    business_type,
    companies_house_number,
    client_money_permission,
    psd_emd_status,
    mlrs_status,
    address_line_1,
    address_line_2,
    address_line_3,
    address_line_4,
    {{ title_case_place("coalesce(town_raw, line4_town, line3_town)") }}      as town,
    (town_raw is null and coalesce(line4_town, line3_town) is not null)       as town_inferred,
    nullif(postcode_raw, '')                                                  as postcode,
    case
        when nullif(postcode_raw, '') is null then null
        when upper(country) like '%UNITED KINGDOM%' or country is null
            then regexp_full_match(postcode_raw, '{{ uk_postcode }}')
    end                                                                       as postcode_valid,
    country,
    website,
    extracted_at,
    loaded_at
from town_candidates
