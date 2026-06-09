create table if not exists content_queue (
  id bigserial primary key,
  content_type text not null check(content_type in (
    'batter_card','pitcher_card','hr_tracker','pitching_index',
    'game_recap','leaderboard','preview','games_of_day',
    'probables_board','insight_tile','text_only','live_event','fantasy_streamer'
  )),
  status text not null default 'draft' check(status in ('draft','approved','rejected','posted','failed')),
  title text,
  tweet_text text,
  image_path text,
  image_url text,
  game_pk bigint,
  player_id bigint,
  player_name text,
  game_date text,
  season integer,
  stage text,
  meta_json text,
  error_message text,
  twitter_post_id text,
  twitter_likes bigint default 0,
  twitter_retweets bigint default 0,
  twitter_replies bigint default 0,
  twitter_impressions bigint default 0,
  created_at text not null default (CURRENT_TIMESTAMP::text),
  reviewed_at text,
  posted_at text,
  content_pillar text,
  hook_type text,
  intended_kpi text,
  priority_score integer default 0,
  campaign text,
  source_module text,
  manual_or_ai text,
  experiment_tag text
);

create index if not exists idx_queue_status on content_queue(status);
create index if not exists idx_queue_date on content_queue(game_date);
create index if not exists idx_queue_player on content_queue(player_id);
create index if not exists idx_queue_content_pillar on content_queue(content_pillar);
create index if not exists idx_queue_intended_kpi on content_queue(intended_kpi);
create index if not exists idx_queue_priority on content_queue(priority_score);
create index if not exists idx_queue_created_at on content_queue(created_at desc);

create table if not exists player_watchlist (
  player_id bigint primary key,
  player_name text not null,
  position text check(position in ('pitcher','batter','two-way')),
  team_id bigint,
  team_abbrev text,
  active integer not null default 1,
  priority integer not null default 5 check(priority between 1 and 10),
  notes text,
  added_at text not null default (CURRENT_TIMESTAMP::text)
);

create table if not exists notification_log (
  id bigserial primary key,
  notification_type text not null,
  channel text not null check(channel in ('email','whatsapp')),
  recipient text not null,
  subject text,
  body_preview text,
  status text not null default 'sent' check(status in ('sent','failed')),
  external_id text,
  sent_at text not null default (CURRENT_TIMESTAMP::text)
);

create table if not exists twitter_metrics_snapshots (
  id bigserial primary key,
  content_queue_id bigint references content_queue(id) on delete cascade,
  snapshot_date text not null,
  likes bigint default 0,
  retweets bigint default 0,
  replies bigint default 0,
  impressions bigint default 0,
  created_at text not null default (CURRENT_TIMESTAMP::text),
  unique(content_queue_id, snapshot_date)
);

create table if not exists live_events (
  id bigserial primary key,
  dedupe_key text not null unique,
  game_pk bigint not null,
  game_date text not null,
  event_type text not null,
  player_id bigint,
  player_name text,
  headline text not null,
  tweet_text text not null,
  payload_json text,
  status text not null default 'new' check(status in ('new','queued','dismissed')),
  queue_id bigint references content_queue(id) on delete set null,
  detected_at text not null default (CURRENT_TIMESTAMP::text)
);

create index if not exists idx_live_events_date on live_events(game_date);
create index if not exists idx_live_events_status on live_events(status);
create index if not exists idx_live_events_game on live_events(game_pk);

create table if not exists security_audit_log (
  id bigserial primary key,
  action text not null,
  result text not null check(result in ('success','failed')),
  content_queue_id bigint references content_queue(id) on delete set null,
  session_id text,
  source_ip text,
  user_agent text,
  details_json text,
  created_at text not null default (CURRENT_TIMESTAMP::text)
);

create index if not exists idx_security_audit_created on security_audit_log(created_at);
create index if not exists idx_security_audit_action on security_audit_log(action);

create table if not exists post_performance (
  id bigserial primary key,
  queue_item_id bigint not null unique references content_queue(id) on delete cascade,
  x_post_id text,
  posted_at text,
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
  engagement_rate double precision default 0,
  bookmark_rate double precision default 0,
  reply_rate double precision default 0,
  repost_rate double precision default 0,
  follows_per_1000_impressions double precision default 0,
  notes text,
  created_at text not null default (CURRENT_TIMESTAMP::text),
  updated_at text not null default (CURRENT_TIMESTAMP::text)
);

create index if not exists idx_post_perf_pillar on post_performance(content_pillar);
create index if not exists idx_post_perf_posted on post_performance(posted_at);

create table if not exists generated_assets (
  id bigserial primary key,
  queue_item_id bigint references content_queue(id) on delete cascade,
  asset_type text not null default 'image',
  local_path text,
  public_url text,
  byte_size bigint,
  checksum text,
  created_at text not null default (CURRENT_TIMESTAMP::text)
);

create table if not exists job_runs (
  id bigserial primary key,
  job_name text not null,
  job_type text not null,
  status text not null default 'running' check(status in ('queued','running','succeeded','failed','cancelled')),
  requested_for_date text,
  season integer,
  stage text,
  started_at text not null default (CURRENT_TIMESTAMP::text),
  finished_at text,
  duration_ms bigint,
  rows_processed bigint,
  files_written bigint,
  error_message text,
  meta_json text
);

create index if not exists idx_job_runs_name_started on job_runs(job_name, started_at desc);
create index if not exists idx_job_runs_status on job_runs(status);
