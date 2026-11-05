/*
  LOAD step of the ELT: land the extractor's JSONL files in DuckDB.

  The JSON is kept as returned by the API, with two storage-saving rules:
    1. The API's self-link fields ("Name", "Individuals", "Permission", ... ) are
       dropped. Each is just https://register.fca.org.uk/.../Firm/<frn>/<thing>,
       fully derivable from the FRN, and they were ~37% of the stored size.
    2. Change-only storage: a firm is re-fetched often, but a new row is stored
       only when its content changed since the previous stored version
       (record_hash, ignoring the API's "System Timestamp"). So the table holds
       every distinct version of every firm, not millions of identical copies.

  Incremental + hash comparison also makes loads idempotent: re-running, or
  re-reading a resumed same-day file, never inserts duplicates.
*/
{{ config(unique_key=none) }}

{% set self_links = [
    "Name", "Individuals", "Requirements", "Permission", "Passport", "Regulators",
    "Appointed Representative", "Address", "Waivers", "Exclusions", "DisciplinaryHistory"
] %}

with files as (
    select
        frn,
        extracted_at,
        json_merge_patch(
            firm,
            '{ {%- for k in self_links -%}"{{ k }}": null{{ ", " if not loop.last }}{%- endfor -%} }'
        )                                   as firm,
        addresses,
        parse_filename(filename)            as source_file
    from read_json(
        '{{ var("raw_files") }}',
        format = 'newline_delimited',
        filename = true,
        columns = {
            'frn': 'VARCHAR',
            'extracted_at': 'TIMESTAMPTZ',
            'firm': 'JSON',
            'addresses': 'JSON'
        }
    )
),

hashed as (
    select
        *,
        md5(
            cast(json_merge_patch(firm, '{"System Timestamp": null}') as varchar)
            || coalesce(cast(addresses as varchar), '')
        ) as record_hash
    from files
),

previous as (
    {% if is_incremental() %}
    select frn, extracted_at, record_hash
    from {{ this }}
    qualify row_number() over (partition by frn order by extracted_at desc) = 1
    {% else %}
    select null::varchar as frn, null::timestamptz as extracted_at, null::varchar as record_hash
    where false
    {% endif %}
),

candidates as (
    -- only extracts newer than what is already stored for that firm
    select h.frn, h.extracted_at, h.record_hash
    from hashed h
    left join previous p on p.frn = h.frn
    where p.extracted_at is null or h.extracted_at > p.extracted_at
),

changes as (
    -- keep a candidate only if it differs from the version just before it
    select frn, extracted_at, record_hash
    from (
        select frn, extracted_at, record_hash, false as is_new from previous
        union all
        select frn, extracted_at, record_hash, true as is_new from candidates
    )
    qualify is_new
        and record_hash is distinct from lag(record_hash) over (partition by frn order by extracted_at)
)

select
    h.frn,
    h.extracted_at,
    h.firm,
    h.addresses,
    h.source_file,
    h.record_hash,
    current_timestamp as loaded_at
from hashed h
semi join changes c
    on c.frn = h.frn
   and c.extracted_at = h.extracted_at
   and c.record_hash = h.record_hash
qualify row_number() over (partition by h.frn, h.extracted_at order by h.source_file) = 1
