{#-
    ============================================================================
    "Did the sender provide a value for this field?", for a VARIANT payload.

    The obvious spelling of that question is wrong on Snowflake, and it is wrong
    quietly. A VARIANT path has three outcomes, not two:

        payload                     payload:quantity     IS NOT NULL
        -------------------------   ------------------   -----------
        {"quantity": 500}           500                  TRUE
        {"quantity": null}          JSON null            TRUE    <-- the trap
        {}                          SQL NULL             FALSE

    JSON null is a value, so IS NOT NULL reports that the field was sent. But
    `::varchar` applied to JSON null yields SQL NULL, so every try_to_* cast over it
    returns NULL as well. int_trade_event_typed decides "malformed" by comparing
    exactly those two facts -- the field was present, yet it is NULL after casting --
    so an explicitly-null optional field satisfies both halves and the event is
    rejected RJ008 for a field the product legitimately has no value for. An upstream
    system that writes "quantity": null on every non-equity product therefore has most
    of its book rejected, with a diagnostic that sends the sender after a defect that
    does not exist.

    IS_NULL_VALUE separates the two: TRUE for JSON null, FALSE for a real value, and
    SQL NULL when the path is absent entirely. Coalescing that NULL to true folds both
    kinds of absence into the single answer the caller asked for.
    ============================================================================
-#}

{% macro payload_has_value(payload_column, field_name) -%}
    not coalesce(is_null_value({{ payload_column }}:{{ field_name }}), true)
{%- endmacro %}
