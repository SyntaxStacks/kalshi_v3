alter table execution_intent
    add column if not exists predicted_fill_probability double precision,
    add column if not exists predicted_slippage_bps double precision,
    add column if not exists predicted_queue_ahead double precision,
    add column if not exists execution_forecast_version text,
    add column if not exists submitted_quantity double precision not null default 0,
    add column if not exists accepted_quantity double precision not null default 0,
    add column if not exists filled_quantity double precision not null default 0,
    add column if not exists cancelled_quantity double precision not null default 0,
    add column if not exists rejected_quantity double precision not null default 0,
    add column if not exists avg_fill_price double precision,
    add column if not exists first_fill_at timestamptz,
    add column if not exists last_fill_at timestamptz,
    add column if not exists terminal_outcome text,
    add column if not exists promotion_state_at_creation text;

create index if not exists idx_execution_intent_mode_processed
    on execution_intent (mode, processed_at desc);

create index if not exists idx_execution_intent_mode_family_processed
    on execution_intent (mode, market_family, processed_at desc);

update execution_intent ei
set predicted_fill_probability = parsed.expected_fill_probability
from (
    select
        e.id,
        max(
            case
                when item like 'fill_probability:%'
                then nullif(split_part(item, ':', 2), '')::double precision
                else null
            end
        ) as expected_fill_probability
    from execution_intent e
    left join lateral jsonb_array_elements_text(e.stop_conditions_json) item on true
    group by e.id
) parsed
where ei.id = parsed.id
  and ei.predicted_fill_probability is null
  and parsed.expected_fill_probability is not null;

update execution_intent ei
set submitted_quantity = coalesce(nullif(ei.submitted_quantity, 0), tl.quantity, 0),
    accepted_quantity = case
        when tl.mode = 'paper' then coalesce(nullif(ei.accepted_quantity, 0), tl.quantity, 0)
        else ei.accepted_quantity
    end,
    filled_quantity = case
        when tl.mode = 'paper' then coalesce(nullif(ei.filled_quantity, 0), tl.quantity, 0)
        else ei.filled_quantity
    end,
    avg_fill_price = coalesce(ei.avg_fill_price, tl.entry_price),
    first_fill_at = case
        when tl.mode = 'paper' then coalesce(ei.first_fill_at, tl.created_at)
        else ei.first_fill_at
    end,
    last_fill_at = case
        when tl.mode = 'paper' then coalesce(ei.last_fill_at, tl.created_at)
        else ei.last_fill_at
    end,
    terminal_outcome = case
        when tl.mode = 'paper' then coalesce(ei.terminal_outcome, 'paper_filled')
        else ei.terminal_outcome
    end
from trade_lifecycle tl
where tl.decision_id = ei.decision_id
  and (
      ei.submitted_quantity = 0
      or ei.accepted_quantity = 0
      or ei.filled_quantity = 0
      or ei.terminal_outcome is null
  );
