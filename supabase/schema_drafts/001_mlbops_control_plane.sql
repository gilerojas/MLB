-- Draft Supabase schema for the MLB Ops control plane.
--
-- Supabase CLI is not installed in this workspace yet, so this is a draft SQL
-- file, not an applied migration. Once the CLI is installed, create the real
-- migration with:
--   supabase migration new mlbops_control_plane
-- then copy this SQL into the generated migration file.

create extension if not exists pgcrypto;

create table if not exists public.content_queue (
  id bigint generated always as identity primary key,
  content_type text not null check (content_type in (
    'batter_card','pitcher_card','hr_tracker','pitching_index',
    'game_recap','leaderboard','preview','games_of_day',
    'probables_board','insight_tile','text_only','live_event','fantasy_streamer'
  )),
  status text not null default 'draft' check (status in (
    'draft','approved','rejected','posted','failed'
  )),
  title text,
  tweet_text text,
  image_path text,
  image_url text,
  storage_bucket text,
  storage_path text,
  game_pk bigint,
  player_id bigint,
  player_name text,
  game_date date,
  season int,
  stage text,
  meta_json jsonb,
  error_message text,
  twitter_post_id text,
  twitter_likes bigint default 0,
  twitter_retweets bigint default 0,
  twitter_replies bigint default 0,
  twitter_impressions bigint default 0,
  reviewed_at timestamptz,
  posted_at timestamptz,
  content_pillar text,
  hook_type text,
  intended_kpi text,
  priority_score int default 0,
  campaign text,
  source_module text,
  manual_or_ai text,
  experiment_tag text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_content_queue_status on public.content_queue(status);
create index if not exists idx_content_queue_game_date on public.content_queue(game_date);
create index if not exists idx_content_queue_player on public.content_queue(player_id);
create index if not exists idx_content_queue_pillar on public.content_queue(content_pillar);
create index if not exists idx_content_queue_kpi on public.content_queue(intended_kpi);
create index if not exists idx_content_queue_priority on public.content_queue(priority_score desc);
create index if not exists idx_content_queue_created on public.content_queue(created_at desc);

create table if not exists public.player_watchlist (
  player_id bigint primary key,
  player_name text not null,
  position text check (position in ('pitcher','batter','two-way')),
  team_id bigint,
  team_abbrev text,
  active boolean not null default true,
  priority int not null default 5 check (priority between 1 and 10),
  notes text,
  added_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_player_watchlist_active_priority
  on public.player_watchlist(active, priority);

create table if not exists public.live_events (
  id bigint generated always as identity primary key,
  dedupe_key text not null unique,
  game_pk bigint not null,
  game_date date not null,
  event_type text not null,
  player_id bigint,
  player_name text,
  headline text not null,
  tweet_text text not null,
  payload_json jsonb,
  status text not null default 'new' check (status in ('new','queued','dismissed')),
  queue_id bigint references public.content_queue(id) on delete set null,
  detected_at timestamptz not null default now()
);

create index if not exists idx_live_events_date on public.live_events(game_date);
create index if not exists idx_live_events_status on public.live_events(status);
create index if not exists idx_live_events_game on public.live_events(game_pk);

create table if not exists public.post_performance (
  id bigint generated always as identity primary key,
  queue_item_id bigint not null unique references public.content_queue(id) on delete cascade,
  x_post_id text,
  posted_at timestamptz,
  content_type text,
  content_pillar text,
  hook_type text,
  intended_kpi text,
  impressions bigint default 0,
  likes bigint default 0,
  replies bigint default 0,
  reposts bigint default 0,
  quote_tweets bigint default 0,
  bookmarks bigint default 0,
  profile_visits bigint default 0,
  follows bigint default 0,
  engagement_rate numeric default 0,
  bookmark_rate numeric default 0,
  reply_rate numeric default 0,
  repost_rate numeric default 0,
  follows_per_1000_impressions numeric default 0,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_post_performance_pillar on public.post_performance(content_pillar);
create index if not exists idx_post_performance_posted on public.post_performance(posted_at);

create table if not exists public.twitter_metrics_snapshots (
  id bigint generated always as identity primary key,
  content_queue_id bigint references public.content_queue(id) on delete cascade,
  snapshot_date date not null,
  likes bigint default 0,
  retweets bigint default 0,
  replies bigint default 0,
  impressions bigint default 0,
  created_at timestamptz not null default now(),
  unique (content_queue_id, snapshot_date)
);

create table if not exists public.notification_log (
  id bigint generated always as identity primary key,
  notification_type text not null,
  channel text not null check (channel in ('email','whatsapp')),
  recipient text not null,
  subject text,
  body_preview text,
  status text not null default 'sent' check (status in ('sent','failed')),
  external_id text,
  sent_at timestamptz not null default now()
);

create table if not exists public.generated_assets (
  id uuid primary key default gen_random_uuid(),
  queue_item_id bigint references public.content_queue(id) on delete cascade,
  asset_type text not null check (asset_type in ('image','csv','json','other')),
  bucket text not null,
  storage_path text not null,
  local_path text,
  public_url text,
  content_type text,
  byte_size bigint,
  checksum text,
  created_at timestamptz not null default now(),
  unique (bucket, storage_path)
);

create index if not exists idx_generated_assets_queue on public.generated_assets(queue_item_id);

create table if not exists public.job_runs (
  id uuid primary key default gen_random_uuid(),
  job_name text not null,
  job_type text not null check (job_type in ('ingest','card_generation','sync','post','maintenance')),
  status text not null default 'running' check (status in ('running','succeeded','failed','cancelled')),
  requested_for_date date,
  season int,
  stage text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  duration_ms bigint,
  rows_processed bigint,
  files_written bigint,
  error_message text,
  meta_json jsonb
);

create index if not exists idx_job_runs_name_started on public.job_runs(job_name, started_at desc);
create index if not exists idx_job_runs_status on public.job_runs(status);

create table if not exists public.security_audit_log (
  id bigint generated always as identity primary key,
  action text not null,
  result text not null check (result in ('success','failed')),
  content_queue_id bigint references public.content_queue(id) on delete set null,
  session_id text,
  source_ip text,
  user_agent text,
  details_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_security_audit_created on public.security_audit_log(created_at);
create index if not exists idx_security_audit_action on public.security_audit_log(action);

alter table public.content_queue enable row level security;
alter table public.player_watchlist enable row level security;
alter table public.live_events enable row level security;
alter table public.post_performance enable row level security;
alter table public.twitter_metrics_snapshots enable row level security;
alter table public.notification_log enable row level security;
alter table public.generated_assets enable row level security;
alter table public.job_runs enable row level security;
alter table public.security_audit_log enable row level security;

-- No anon/authenticated policies yet. The first implementation should access
-- these tables from the FastAPI worker with a server-side Supabase service role.
-- Add user-facing RLS policies only after the auth model is explicit.

insert into storage.buckets (id, name, public)
values ('mlbops-generated', 'mlbops-generated', false)
on conflict (id) do nothing;

