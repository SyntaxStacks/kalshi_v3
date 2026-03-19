create table if not exists operator_control_state (
    control_key text primary key,
    live_order_placement_enabled boolean not null default true,
    updated_by text,
    note text,
    updated_at timestamptz not null default now()
);

insert into operator_control_state (control_key, live_order_placement_enabled, updated_by, note)
values ('global', true, 'migration', 'default operator control state')
on conflict (control_key) do nothing;

alter table execution_intent
    add column if not exists market_ticker text;

alter table execution_intent
    add column if not exists side text;

alter table execution_intent
    add column if not exists client_order_id text;

alter table execution_intent
    add column if not exists exchange_order_id text;

alter table execution_intent
    add column if not exists fill_status text;

alter table execution_intent
    add column if not exists submitted_at timestamptz;

alter table execution_intent
    add column if not exists acknowledged_at timestamptz;

alter table execution_intent
    add column if not exists cancelled_at timestamptz;

alter table execution_intent
    add column if not exists last_transition_at timestamptz not null default now();

create table if not exists execution_intent_transition (
    id bigserial primary key,
    intent_id bigint not null references execution_intent(id) on delete cascade,
    from_status text,
    to_status text not null,
    last_error text,
    details_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_execution_intent_transition_intent_created
    on execution_intent_transition (intent_id, created_at desc);

create index if not exists idx_execution_intent_mode_status_created
    on execution_intent (mode, status, created_at asc);

create index if not exists idx_execution_intent_client_order_id
    on execution_intent (client_order_id);

create index if not exists idx_execution_intent_exchange_order_id
    on execution_intent (exchange_order_id);
