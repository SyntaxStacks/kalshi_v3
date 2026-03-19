alter table lane_state
    add column if not exists promotion_reason text;

create table if not exists lane_state_transition (
    id bigserial primary key,
    lane_key text not null,
    from_state text,
    to_state text not null,
    from_reason text,
    to_reason text,
    current_champion_model text,
    details_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_lane_state_transition_lane_created
    on lane_state_transition (lane_key, created_at desc);
