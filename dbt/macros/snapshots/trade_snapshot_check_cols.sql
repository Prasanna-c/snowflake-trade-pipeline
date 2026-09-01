{#-
    ============================================================================
    The columns of FCT_TRADE whose change opens a new SNP_TRADE row.

    WHY THIS IS A MACRO AND NOT A LITERAL IN THE SNAPSHOT CONFIG.

    Two places need this exact list:

      snapshots/snp_trade.sql                                the `check` strategy itself
      tests/singular/assert_snapshot_covers_material_columns.sql
                                                             asserts the list still covers
                                                             every material column

    Held as two hand-maintained copies, they drift -- and the drift is invisible in the
    worst way. The test would keep passing against its own stale copy while the snapshot
    quietly stopped historising a column, which is precisely the failure the test was
    written to prevent. One definition, two readers, no drift.

    MEMBERSHIP RULE.

    A column belongs here if a change to it means the trade itself changed. Deliberately
    absent are:

      * derived columns, because their inputs are listed -- historising tenor_days as well
        as trade_date and maturity_date costs a wider comparison on every run and records
        nothing the inputs do not already tell you;
      * write metadata such as dbt_updated_at, which changes on every run and would
        produce a new row per trade per run.

    Every omission is recorded with its reason in the exclusion list of the test above, so
    adding a column to FCT_TRADE forces a decision rather than a silent default.
    ============================================================================
-#}

{% macro trade_snapshot_check_cols() %}

    {{ return([
        'current_version',
        'lifecycle_status',
        'notional_amount',
        'notional_currency',
        'settlement_currency',
        'buy_sell',
        'quantity',
        'price',
        'trade_date',
        'settlement_date',
        'maturity_date',
        'counterparty_id',
        'book_id',
        'trader_id',
        'product_type',
        'clearing_house',
        'legal_entity',
        'source_system',
        'is_limit_breach'
    ]) }}

{% endmacro %}
