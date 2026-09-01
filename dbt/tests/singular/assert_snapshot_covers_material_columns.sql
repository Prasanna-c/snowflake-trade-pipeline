/*
    The snapshot's `check_cols` list must cover every material column of FCT_TRADE.

    SNP_TRADE uses the `check` strategy with an explicit column list, because
    `check_cols='all'` would include dbt_updated_at -- which changes on every run -- and
    would therefore write a new snapshot row per trade per run, forever.

    The cost of an explicit list is that it must be maintained. Add a material column to
    FCT_TRADE, forget the snapshot, and changes to that column stop being historised. Nothing
    fails; the snapshot simply becomes quietly incomplete, and the gap is discovered when
    someone tries to answer an audit question about that column.

    This test closes that gap by comparing the declared list against FCT_TRADE's actual
    columns from INFORMATION_SCHEMA, minus a documented exclusion list. Adding a column now
    forces a decision: either historise it, or add it to the exclusions with a reason.

    Reading INFORMATION_SCHEMA rather than the dbt manifest is deliberate: the manifest
    describes what dbt intended to build, and this test needs to know what actually exists.
*/

{{ config(severity = 'error', tags = ['metadata', 'audit']) }}

{#-
    The same definition the snapshot's `check` strategy uses, not a copy of it. A copy
    would let the test pass against a stale list while the snapshot historised something
    different.
-#}
{%- set snapshot_check_cols = trade_snapshot_check_cols() -%}

{#-
    Columns deliberately NOT historised, each for a stated reason. An unexplained entry
    here would defeat the purpose of the test.
-#}
{%- set excluded_columns = {
    'trade_id':                    'The snapshot key itself.',
    'dbt_updated_at':              'Changes every run; would create a row per run.',
    'dbt_invocation_id':           'Changes every run.',
    'status_changed_at':           'Derived from lifecycle_status, which IS checked.',
    'is_expiry_sweep_update':      'Metadata about the write, not about the trade.',
    'days_to_maturity':            'Derived from maturity_date, which IS checked. Changes daily by construction.',
    'is_expiring_soon':            'Derived from maturity_date. Changes daily by construction.',
    'is_live':                     'Derived from lifecycle_status.',
    'is_expired':                  'Derived from lifecycle_status.',
    'is_cancelled':                'Derived from lifecycle_status.',
    'signed_notional_amount':      'Derived from notional_amount and buy_sell, both checked.',
    'tenor_days':                  'Derived from trade_date and maturity_date, both checked.',
    'last_event_timestamp':        'Provenance of the write, not an economic attribute.',
    'last_source_file_name':       'Provenance.',
    'last_batch_id':               'Provenance.',
    'last_event_sk':               'Provenance.',
    'last_action':                 'Implied by lifecycle_status and current_version.',
    'last_version_action':         'Implied by current_version.',
    'uti':                         'Immutable for the life of the trade; a change would be a new trade.',
    'asset_class':                 'Functionally determined by product_type, which IS checked.',
    'counterparty_name':           'Reference attribute; historised by the counterparty dimension, not here.',
    'counterparty_lei':            'Reference attribute.',
    'counterparty_country_code':   'Reference attribute.',
    'counterparty_credit_rating':  'Reference attribute; changes with ratings actions, not with the trade.',
    'book_name':                   'Reference attribute.',
    'desk':                        'Reference attribute, determined by book_id which IS checked.',
    'execution_venue':             'Immutable execution fact.',
    'is_duplicate_uti':            'A detection flag, not a trade attribute.',
    'warning_rule_codes':          'Adjudication metadata; historised in AUDIT.TRADE_RULE_RESULT.'
} -%}

with actual_columns as (

    select lower(column_name) as column_name
    from {{ target.database }}.information_schema.columns
    where lower(table_schema) = lower('{{ generate_schema_name("core", none) }}')
        and lower(table_name) = lower('{{ ref("fct_trade").identifier }}')

),

declared as (

    select column1 as column_name
    from
        values
    {%- for col in snapshot_check_cols %}
    ('{{ col }}'){{ "," if not loop.last }} -- noqa: LT02
    
{%- endfor %}

),

excluded as (

    select column1 as column_name
    from
        values
    {%- for col in excluded_columns.keys() %}
    ('{{ col }}'){{ "," if not loop.last }} -- noqa: LT02
    
{%- endfor %}

),

-- A material column of FCT_TRADE that is neither historised nor explicitly excluded.
uncovered as (

    select
        actual_columns.column_name,
        'MATERIAL_COLUMN_NOT_IN_SNAPSHOT_CHECK_COLS' as discrepancy
    from actual_columns
    left join declared on actual_columns.column_name = declared.column_name
    left join excluded on actual_columns.column_name = excluded.column_name
    where declared.column_name is null
        and excluded.column_name is null

),

-- Declared in check_cols but no longer a column of FCT_TRADE. The snapshot would fail at
-- runtime, but this reports it at test time with a clear message instead.
stale_declaration as (

    select
        declared.column_name,
        'CHECK_COL_DOES_NOT_EXIST_ON_FCT_TRADE' as discrepancy
    from declared
    left join actual_columns on declared.column_name = actual_columns.column_name
    where actual_columns.column_name is null

)

select * from uncovered
union all
select * from stale_declaration
