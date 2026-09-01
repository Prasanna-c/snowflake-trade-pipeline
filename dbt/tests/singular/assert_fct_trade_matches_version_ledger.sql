/*
    FCT_TRADE must be exactly the maximum-version projection of FCT_TRADE_VERSION.

    They are built by separate incremental models with separate watermarks, and that is where
    divergence comes from: one advances, the other does not, and the golden record starts
    reporting economics from a version the ledger says was superseded. Nothing else detects
    this -- both tables individually pass every test they have.

    Three failure modes are checked:

      MISSING_FROM_LEDGER   FCT_TRADE holds a version the ledger has no record of.
      VERSION_MISMATCH      The two disagree about which version is current.
      ECONOMICS_MISMATCH    They agree on the version but disagree about the trade.

    The third is the most valuable, because it is the one a reviewer would never think to
    look for. It happens when the expiry sweep's carry-through of economics diverges from the
    ledger -- for example if a column is added to FCT_TRADE_VERSION and to the new-events
    branch of FCT_TRADE, but not to the expiry-sweep branch, which is an easy omission in a
    model with two union branches.
*/

{{ config(severity = 'error', tags = ['consistency', 'critical']) }}

with ledger_max as (

    select
        trade_id,
        max(trade_version) as max_version
    from {{ ref('fct_trade_version') }}
    where trade_id is not null
    group by trade_id

),

ledger_current as (

    select
        ledger.trade_id,
        ledger.trade_version,
        ledger.notional_amount,
        ledger.notional_currency,
        ledger.buy_sell,
        ledger.maturity_date,
        ledger.counterparty_id,
        ledger.book_id,
        ledger.product_type
    from {{ ref('fct_trade_version') }} as ledger
    inner join ledger_max
        on ledger.trade_id = ledger_max.trade_id
            and ledger.trade_version = ledger_max.max_version

),

golden as (

    select
        trade_id,
        current_version,
        notional_amount,
        notional_currency,
        buy_sell,
        maturity_date,
        counterparty_id,
        book_id,
        product_type
    from {{ ref('fct_trade') }}

),

discrepancies as (

    select
        golden.trade_id,
        golden.current_version as golden_version,
        ledger_current.trade_version as ledger_version,
        golden.notional_amount as golden_notional,
        ledger_current.notional_amount as ledger_notional,
        case
            when ledger_current.trade_id is null
                then 'MISSING_FROM_LEDGER'
            when golden.current_version <> ledger_current.trade_version
                then 'VERSION_MISMATCH'
            else 'ECONOMICS_MISMATCH'
        end as discrepancy

    from golden
    left join ledger_current
        on golden.trade_id = ledger_current.trade_id

    where
        ledger_current.trade_id is null
        or golden.current_version <> ledger_current.trade_version
        -- equal_null so that a null on both sides counts as agreement. Plain <> would
        -- report every perpetual trade (null maturity) as a mismatch.
        or not equal_null(golden.notional_amount, ledger_current.notional_amount)
        or not equal_null(golden.notional_currency, ledger_current.notional_currency)
        or not equal_null(golden.buy_sell, ledger_current.buy_sell)
        or not equal_null(golden.maturity_date, ledger_current.maturity_date)
        or not equal_null(golden.counterparty_id, ledger_current.counterparty_id)
        or not equal_null(golden.book_id, ledger_current.book_id)
        or not equal_null(golden.product_type, ledger_current.product_type)

)

select * from discrepancies
