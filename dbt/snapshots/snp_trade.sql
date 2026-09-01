{#-
    ============================================================================
    Slowly-changing-dimension type 2 history of the golden record.

    WHY THIS EXISTS WHEN FCT_TRADE_VERSION ALREADY KEEPS EVERY VERSION.

    They answer genuinely different questions, and the difference is the reason a version
    ledger alone is not sufficient for audit:

      FCT_TRADE_VERSION  what the upstream system told us, and when it told us.
                         Grain: (trade_id, trade_version). Business history.

      SNP_TRADE          what OUR WAREHOUSE believed, and for exactly how long.
                         Grain: (trade_id, valid_from, valid_to). System history.

    Two things appear only here:

      1. State changes with no corresponding trade event. The expiry sweep transitions a
         trade from LIVE to EXPIRED because time passed, not because a message arrived.
         There is no new trade_version, so the version ledger shows nothing -- but the
         warehouse's belief about the trade changed, and this snapshot records it.

      2. Point-in-time reconstruction. "Show me the book exactly as it stood at close on
         14 August" is a `where valid_from <= ts and (valid_to > ts or valid_to is null)`
         filter here. From the version ledger it requires reasoning about which version
         was current at that instant, which is both slow and easy to get subtly wrong.

    STRATEGY: `check` rather than `timestamp`.

    A timestamp strategy needs a column that changes whenever anything material changes.
    FCT_TRADE has dbt_updated_at, but that is set to current_timestamp() on every run --
    so a timestamp strategy would create a new snapshot row on every single run, for every
    trade, forever. The check strategy compares the listed columns and writes a row only
    when one actually changed.

    The column list is explicit rather than `check_cols='all'`. With 'all', dbt_updated_at
    would be included and we would be back to a new row per run. Explicit lists have a
    maintenance cost -- a new economic column must be added to the list -- so the list lives
    in macros/snapshots/trade_snapshot_check_cols.sql, where the membership rule is stated
    and where tests/singular/assert_snapshot_covers_material_columns.sql reads the same
    definition rather than a copy of it. That test fails if a material column in FCT_TRADE
    is missing from the list.

    invalidate_hard_deletes: a trade cannot be deleted from FCT_TRADE (the merge never
    deletes), so this is off. Turning it on would add a full anti-join on every run to
    detect deletions that cannot happen.
    ============================================================================
-#}

{% snapshot snp_trade %}

{{
    config(
        target_schema = generate_schema_name('snapshots', none),
        unique_key = 'trade_id',
        strategy = 'check',
        check_cols = trade_snapshot_check_cols(),
        invalidate_hard_deletes = false,
        tags = ['snapshot', 'audit', 'compliance']
    )
}}

select
    trade_id,
    current_version,
    last_action,
    last_version_action,
    uti,

    product_type,
    asset_class,
    buy_sell,
    notional_amount,
    notional_currency,
    settlement_currency,
    signed_notional_amount,
    quantity,
    price,

    trade_date,
    settlement_date,
    maturity_date,
    tenor_days,
    days_to_maturity,

    counterparty_id,
    counterparty_name,
    counterparty_credit_rating,
    book_id,
    book_name,
    desk,
    trader_id,
    execution_venue,
    clearing_house,
    legal_entity,

    lifecycle_status,
    is_live,
    is_expired,
    is_cancelled,
    is_limit_breach,
    is_duplicate_uti,

    -- Carried through so a snapshot row can be explained. Without it, an analyst looking
    -- at a LIVE -> EXPIRED transition has no way to tell whether a message arrived or
    -- time simply passed.
    is_expiry_sweep_update,
    status_changed_at,

    source_system,
    last_event_timestamp,
    last_batch_id,
    dbt_invocation_id

from {{ ref('fct_trade') }}

{% endsnapshot %}
