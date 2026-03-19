create table if not exists ingest_event_log (
    id bigserial primary key,
    source text not null,
    topic text not null,
    entity_key text not null,
    payload_json jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now()
);

create index if not exists idx_ingest_event_log_source_topic_observed
    on ingest_event_log (source, topic, observed_at desc, id desc);

create index if not exists idx_ingest_event_log_entity_observed
    on ingest_event_log (entity_key, observed_at desc, id desc);
