create table if not exists worker_status (
    service text primary key,
    status text not null,
    last_started_at timestamptz,
    last_succeeded_at timestamptz,
    last_failed_at timestamptz,
    last_error text,
    details_json jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

create index if not exists idx_worker_status_updated_at
    on worker_status (updated_at desc);
