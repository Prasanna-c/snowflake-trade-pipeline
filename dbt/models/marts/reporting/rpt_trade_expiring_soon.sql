/*
    Trades maturing within the next `expiring_soon_days` days, plus anything that has
    matured but is still showing LIVE.

    The second half is the important half. A row with `is_overdue_for_expiry = true` means
    the trade passed its maturity date and the expiry sweep in FCT_TRADE has not run --
    which means dbt has not run, which means the whole curated layer is stale. It is the
    cheapest possible canary for "the pipeline stopped and nobody noticed", and it is the
    same condition ALERT_EXPIRY_OVERDUE fires on.

    Operationally this is also the settlement team's working list, ordered so the most
    urgent trades are at the top without anyone having to sort.
*/

{{
    config(
        materialized = 'view',
        tags = ['reporting', 'dashboard', 'operations']
    )
}}

{% set business_date = "to_date('" ~ var('business_date') ~ "')" %}

with live_and_maturing as (

    select * from {{ ref('fct_trade') }}
    where maturity_date is not null
      and (
          -- Maturing within the window.
          (
              lifecycle_status = 'LIVE'
              and maturity_date between {{ business_date }}
                  and dateadd('day', {{ var('expiring_soon_days') }}, {{ business_date }})
          )
          -- Or already matured but not yet transitioned -- the canary.
          or (lifecycle_status = 'LIVE' and maturity_date < {{ business_date }})
      )

),

final as (

    select
        live_and_maturing.trade_id,
        live_and_maturing.current_version,
        live_and_maturing.uti,
        live_and_maturing.product_type,
        live_and_maturing.asset_class,
        live_and_maturing.buy_sell,
        live_and_maturing.notional_amount,
        live_and_maturing.notional_currency,
        live_and_maturing.settlement_currency,

        live_and_maturing.trade_date,
        live_and_maturing.settlement_date,
        live_and_maturing.maturity_date,
        live_and_maturing.days_to_maturity,

        live_and_maturing.counterparty_id,
        live_and_maturing.counterparty_name,
        live_and_maturing.counterparty_credit_rating,
        live_and_maturing.book_id,
        live_and_maturing.book_name,
        live_and_maturing.desk,
        live_and_maturing.trader_id,
        live_and_maturing.legal_entity,
        live_and_maturing.clearing_house,

        live_and_maturing.lifecycle_status,

        -- The canary flag.
        live_and_maturing.maturity_date < {{ business_date }} as is_overdue_for_expiry,

        case
            when live_and_maturing.maturity_date < {{ business_date }} then 'OVERDUE'
            when live_and_maturing.days_to_maturity = 0 then 'MATURES_TODAY'
            when live_and_maturing.days_to_maturity <= 2 then 'IMMINENT'
            else 'UPCOMING'
        end as urgency,

        live_and_maturing.source_system,
        live_and_maturing.last_event_timestamp,
        current_timestamp() as dbt_updated_at

    from live_and_maturing

)

select *
from final
-- Ordered in the view itself. Snowflake does not preserve order through a view in
-- general, but for an operational worklist that is read directly it costs nothing and
-- means the urgent rows are on the first screen without the consumer sorting.
order by
    case urgency
        when 'OVERDUE' then 1
        when 'MATURES_TODAY' then 2
        when 'IMMINENT' then 3
        else 4
    end,
    notional_amount desc
