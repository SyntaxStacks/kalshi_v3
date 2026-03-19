create table if not exists notification_delivery (
    id bigserial primary key,
    notification_kind text not null,
    fingerprint text not null,
    payload_json jsonb not null default '{}'::jsonb,
    delivered_at timestamptz not null default now(),
    unique (notification_kind, fingerprint)
);

create index if not exists idx_notification_delivery_kind_delivered
    on notification_delivery (notification_kind, delivered_at desc);

create table if not exists operator_action_event (
    id bigserial primary key,
    action text not null,
    actor text,
    note text,
    payload_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_operator_action_event_created
    on operator_action_event (created_at desc);
