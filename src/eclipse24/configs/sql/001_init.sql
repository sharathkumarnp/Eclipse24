-- src/eclipse24/configs/sql/001_state.sql
-- ===========================================
-- Core Safeguard DB Schema (servers, slack, actions, extensions, reminders)
-- ===========================================

create table if not exists servers
(
    server_id
    text
    primary
    key,
    server_name
    text,
    owner_email
    text,
    region
    text,
    status
    text,
    creation_date
    timestamptz,
    last_seen_at
    timestamptz
    not
    null
    default
    now
(
)
    );

do
$$
begin
create type place_enum as enum('channel','dm','manager_channel','manager_dm');
exception when duplicate_object then null;
end $$;

create table if not exists slack_messages
(
    server_id
    text
    references
    servers
(
    server_id
),
    place place_enum not null,
    channel text not null,
    ts text not null,
    created_at timestamptz not null default now
(
),
    primary key
(
    server_id,
    place
)
    );

do
$$
begin
create type action_enum as enum('posted','approve','extend_request','extend_approve','extend_deny','auto_block','reminder');
exception when duplicate_object then null;
end $$;

create table if not exists actions
(
    id
    bigserial
    primary
    key,
    server_id
    text
    references
    servers
(
    server_id
),
    action action_enum not null,
    actor_uid text,
    actor_name text,
    meta jsonb,
    created_at timestamptz not null default now
(
)
    );

create table if not exists extensions
(
    server_id
    text
    primary
    key
    references
    servers
(
    server_id
),
    extension_hours int not null,
    expires_at timestamptz not null,
    granted_by_uid text,
    granted_by text,
    updated_at timestamptz not null default now
(
)
    );

create table if not exists reminders
(
    server_id
    text
    primary
    key
    references
    servers
(
    server_id
),
    last_reminded_at timestamptz not null
    );

-- ===========================================
-- NEW TABLES FOR VALIDATOR + AUDIT
-- ===========================================

do
$$
begin
create type state_enum as enum('PENDING','EXTENDED','BLOCKED','EXPIRED');
exception when duplicate_object then null;
end $$;

create table if not exists server_state
(
    server_id
    text
    primary
    key
    references
    servers
(
    server_id
),
    status state_enum not null,
    until timestamptz null,
    updated_at timestamptz not null default now
(
)
    );

create table if not exists audit_log
(
    id
    uuid
    default
    gen_random_uuid
(
) primary key,
    server_id text references servers
(
    server_id
),
    action text not null,
    actor text,
    details jsonb,
    created_at timestamptz not null default now
(
)
    );