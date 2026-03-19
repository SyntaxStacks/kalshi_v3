alter table execution_intent
    add column if not exists status text not null default 'pending';

alter table execution_intent
    add column if not exists last_error text;

alter table execution_intent
    add column if not exists processed_at timestamptz;

create index if not exists idx_execution_intent_status_created
    on execution_intent (status, created_at asc);
