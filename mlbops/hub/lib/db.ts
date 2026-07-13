/**
 * Hub-side database helpers.
 *
 * Local development defaults to SQLite via better-sqlite3. Production can use
 * Postgres by setting MLBOPS_DB_BACKEND=postgres and DATABASE_URL.
 */
import Database from "better-sqlite3";
import path from "path";
import { Pool } from "pg";

const DB_BACKEND = (process.env.MLBOPS_DB_BACKEND || "sqlite").toLowerCase();
const USE_POSTGRES = ["postgres", "postgresql", "pg"].includes(DB_BACKEND);
const DB_PATH =
  process.env.HUB_DB_PATH ||
  path.join(process.cwd(), "..", "..", "data", "hub.db");

let _sqlite: Database.Database | null = null;
let _pool: Pool | null = null;

function getSqlite(): Database.Database {
  if (!_sqlite) {
    _sqlite = new Database(DB_PATH);
    _sqlite.pragma("journal_mode = WAL");
    ensureSecurityTables(_sqlite);
  }
  return _sqlite;
}

function getPool(): Pool {
  if (!_pool) {
    if (!process.env.DATABASE_URL) {
      throw new Error("MLBOPS_DB_BACKEND=postgres requires DATABASE_URL");
    }
    _pool = new Pool({ connectionString: process.env.DATABASE_URL });
  }
  return _pool;
}

export function getDb(): Database.Database {
  if (USE_POSTGRES) {
    throw new Error("getDb() is SQLite-only. Use exported async helpers in Postgres mode.");
  }
  return getSqlite();
}

function ensureSecurityTables(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS security_audit_log (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      action              TEXT NOT NULL,
      result              TEXT NOT NULL CHECK(result IN ('success','failed')),
      content_queue_id    INTEGER,
      session_id          TEXT,
      source_ip           TEXT,
      user_agent          TEXT,
      details_json        TEXT,
      created_at          TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_security_audit_created ON security_audit_log(created_at);
    CREATE INDEX IF NOT EXISTS idx_security_audit_action ON security_audit_log(action);
  `);
}

function q(sql: string): string {
  let idx = 0;
  return sql
    .replace(/datetime\('now'\)/g, "CURRENT_TIMESTAMP")
    .replace(/\?/g, () => `$${++idx}`);
}

async function all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (USE_POSTGRES) {
    const res = await getPool().query(q(sql), params);
    return res.rows as T[];
  }
  return getSqlite().prepare(sql).all(...params) as T[];
}

async function one<T>(sql: string, params: unknown[] = []): Promise<T | null> {
  if (USE_POSTGRES) {
    const res = await getPool().query(q(sql), params);
    return (res.rows[0] as T) || null;
  }
  return (getSqlite().prepare(sql).get(...params) as T) || null;
}

async function run(sql: string, params: Record<string, unknown> | unknown[] = []): Promise<{ lastID?: number }> {
  if (USE_POSTGRES) {
    if (Array.isArray(params)) {
      const res = await getPool().query(q(sql), params);
      return { lastID: res.rows[0]?.id };
    }
    const paramIndex = new Map<string, number>();
    const values: unknown[] = [];
    const text = sql.replace(/@([A-Za-z_][A-Za-z0-9_]*)/g, (_, key: string) => {
      const existing = paramIndex.get(key);
      if (existing) return `$${existing}`;
      values.push((params as Record<string, unknown>)[key]);
      const next = values.length;
      paramIndex.set(key, next);
      return `$${next}`;
    });
    const returning = /insert\s+into\s+content_queue/i.test(text) && !/returning/i.test(text)
      ? `${text} RETURNING id`
      : text;
    const res = await getPool().query(returning, values);
    return { lastID: res.rows[0]?.id };
  }
  const result = Array.isArray(params)
    ? getSqlite().prepare(sql).run(...params)
    : getSqlite().prepare(sql).run(params);
  return { lastID: Number(result.lastInsertRowid) };
}

export interface QueueItem {
  id: number;
  content_type: string;
  status: string;
  title: string | null;
  tweet_text: string | null;
  image_path: string | null;
  image_url: string | null;
  game_pk: number | null;
  player_id: number | null;
  player_name: string | null;
  game_date: string | null;
  season: number | null;
  stage: string | null;
  meta_json: string | null;
  error_message: string | null;
  twitter_post_id: string | null;
  twitter_likes: number;
  twitter_retweets: number;
  twitter_replies: number;
  twitter_impressions: number;
  content_pillar: string | null;
  hook_type: string | null;
  intended_kpi: string | null;
  priority_score: number | null;
  campaign: string | null;
  source_module: string | null;
  manual_or_ai: string | null;
  experiment_tag: string | null;
  created_at: string;
  reviewed_at: string | null;
  posted_at: string | null;
}

export interface AuditEventInput {
  action: string;
  result: "success" | "failed";
  content_queue_id?: number | null;
  session_id?: string | null;
  source_ip?: string | null;
  user_agent?: string | null;
  details_json?: string | null;
}

export interface WatchlistPlayer {
  player_id: number;
  player_name: string;
  position: string | null;
  team_id: number | null;
  team_abbrev: string | null;
  active: number;
  priority: number;
  notes: string | null;
  added_at: string;
}

export async function getPendingQueue(limit = 20, offset = 0): Promise<QueueItem[]> {
  return all<QueueItem>(
    `SELECT * FROM content_queue WHERE status = 'draft'
     ORDER BY created_at DESC LIMIT ? OFFSET ?`,
    [limit, offset],
  );
}

export async function getQueueByStatus(status: string, limit = 20, offset = 0): Promise<QueueItem[]> {
  return all<QueueItem>(
    `SELECT * FROM content_queue WHERE status = ?
     ORDER BY created_at DESC LIMIT ? OFFSET ?`,
    [status, limit, offset],
  );
}

export async function getQueueItem(id: number): Promise<QueueItem | null> {
  return one<QueueItem>("SELECT * FROM content_queue WHERE id = ?", [id]);
}

export async function updateQueueItem(
  id: number,
  fields: Partial<{
    status: string;
    tweet_text: string;
    twitter_post_id: string;
    twitter_likes: number;
    twitter_retweets: number;
    twitter_replies: number;
    twitter_impressions: number;
    reviewed_at: string;
    posted_at: string;
    error_message: string;
    meta_json: string;
    content_pillar: string;
    hook_type: string;
    intended_kpi: string;
    priority_score: number;
    campaign: string;
    source_module: string;
    manual_or_ai: string;
    experiment_tag: string;
  }>
): Promise<void> {
  const entries = Object.entries(fields);
  if (!entries.length) return;
  if (USE_POSTGRES) {
    const sets = entries.map(([k], i) => `${k} = $${i + 1}`).join(", ");
    await getPool().query(`UPDATE content_queue SET ${sets} WHERE id = $${entries.length + 1}`, [
      ...entries.map(([, v]) => v),
      id,
    ]);
    return;
  }
  const sets = entries.map(([k]) => `${k} = @${k}`).join(", ");
  getSqlite().prepare(`UPDATE content_queue SET ${sets} WHERE id = @id`).run({ ...fields, id });
}

export async function insertQuickPost(tweetText: string, meta: Record<string, unknown>): Promise<number> {
  const now = new Date().toISOString();
  const result = await run(
    `INSERT INTO content_queue
       (content_type, status, title, tweet_text, image_path, image_url, game_date, season, stage, meta_json,
        content_pillar, hook_type, intended_kpi, priority_score, campaign, source_module, manual_or_ai, experiment_tag,
        created_at)
     VALUES
       ('text_only', 'draft', 'Quick post', @tweet_text, '', '', @game_date, @season, 'regular_season', @meta_json,
        'text_only', 'debate_prompt', 'replies', 50, 'daily_mlb', 'quick_post', 'manual', '',
        @created_at)`,
    {
      tweet_text: tweetText,
      game_date: now.slice(0, 10),
      season: new Date().getFullYear(),
      meta_json: JSON.stringify(meta),
      created_at: now,
    },
  );
  return Number(result.lastID);
}

export async function logAuditEvent(input: AuditEventInput): Promise<void> {
  await run(
    `INSERT INTO security_audit_log
       (action, result, content_queue_id, session_id, source_ip, user_agent, details_json)
     VALUES
       (@action, @result, @content_queue_id, @session_id, @source_ip, @user_agent, @details_json)`,
    {
      action: input.action,
      result: input.result,
      content_queue_id: input.content_queue_id ?? null,
      session_id: input.session_id ?? null,
      source_ip: input.source_ip ?? null,
      user_agent: input.user_agent ?? null,
      details_json: input.details_json ?? null,
    },
  );
}

function parseMeta(metaJson: string | null): Record<string, unknown> {
  if (!metaJson) return {};
  try {
    const parsed = JSON.parse(metaJson);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

export async function mergeQueueMeta(id: number, patch: Record<string, unknown>): Promise<void> {
  const item = await getQueueItem(id);
  if (!item) return;
  const meta = { ...parseMeta(item.meta_json), ...patch };
  await updateQueueItem(id, { meta_json: JSON.stringify(meta) });
}

export async function getPostingStreakStats(): Promise<{
  posted_today: number;
  weekly_total: number;
  current_streak: number;
  longest_streak: number;
  manual_ratio: number;
}> {
  const rows = await all<{ day: string; meta_json: string | null }>(
    `SELECT ${USE_POSTGRES ? "CAST(posted_at AS date)::text" : "date(posted_at)"} AS day, meta_json
     FROM content_queue
     WHERE status = 'posted' AND posted_at IS NOT NULL
     ORDER BY ${USE_POSTGRES ? "CAST(posted_at AS date)" : "date(posted_at)"} DESC, posted_at DESC`,
  );

  const today = new Date().toISOString().slice(0, 10);
  const daySet = new Set(rows.map((r) => r.day).filter(Boolean));
  const postedToday = rows.filter((r) => r.day === today).length;
  const weekAgo = new Date();
  weekAgo.setDate(weekAgo.getDate() - 6);
  const weekStart = weekAgo.toISOString().slice(0, 10);
  const weeklyTotal = rows.filter((r) => r.day >= weekStart).length;

  let current = 0;
  const cursor = new Date(`${today}T00:00:00.000Z`);
  for (;;) {
    const day = cursor.toISOString().slice(0, 10);
    if (!daySet.has(day)) break;
    current += 1;
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  }

  let longest = 0;
  let runLength = 0;
  let previous: string | null = null;
  for (const day of Array.from(daySet).sort()) {
    if (!previous) {
      runLength = 1;
    } else {
      const prevDate = new Date(`${previous}T00:00:00.000Z`);
      prevDate.setUTCDate(prevDate.getUTCDate() + 1);
      runLength = prevDate.toISOString().slice(0, 10) === day ? runLength + 1 : 1;
    }
    previous = day;
    longest = Math.max(longest, runLength);
  }

  const recentRows = rows.slice(0, 30);
  const manualCount = recentRows.filter((r) => {
    const meta = parseMeta(r.meta_json);
    return meta.creation_mode === "manual" || meta.ai_assisted === false;
  }).length;

  return {
    posted_today: postedToday,
    weekly_total: weeklyTotal,
    current_streak: current,
    longest_streak: longest,
    manual_ratio: recentRows.length ? Math.round((manualCount / recentRows.length) * 100) : 0,
  };
}

export async function getWatchlist(activeOnly = true): Promise<WatchlistPlayer[]> {
  const where = activeOnly ? "WHERE active = 1" : "";
  return all<WatchlistPlayer>(`SELECT * FROM player_watchlist ${where} ORDER BY priority ASC`);
}

export async function upsertWatchlistPlayer(player: Omit<WatchlistPlayer, "added_at">): Promise<void> {
  await run(
    `INSERT INTO player_watchlist
       (player_id, player_name, position, team_id, team_abbrev, active, priority, notes)
     VALUES
       (@player_id, @player_name, @position, @team_id, @team_abbrev, @active, @priority, @notes)
     ON CONFLICT(player_id) DO UPDATE SET
       player_name = excluded.player_name,
       position    = excluded.position,
       team_abbrev = excluded.team_abbrev,
       active      = excluded.active,
       priority    = excluded.priority,
       notes       = excluded.notes`,
    player,
  );
}

export async function getQueueCounts(): Promise<Record<string, number>> {
  const rows = await all<{ status: string; count: number }>(
    "SELECT status, COUNT(*) as count FROM content_queue GROUP BY status",
  );
  return Object.fromEntries(rows.map((r) => [r.status, Number(r.count)]));
}

export async function getRecentPosted(days = 7): Promise<QueueItem[]> {
  if (USE_POSTGRES) {
    return all<QueueItem>(
      `SELECT * FROM content_queue
       WHERE status = 'posted'
         AND posted_at::timestamp >= CURRENT_TIMESTAMP + ($1)::interval
       ORDER BY posted_at DESC`,
      [`-${days} days`],
    );
  }
  return all<QueueItem>(
    `SELECT * FROM content_queue
     WHERE status = 'posted'
       AND posted_at >= datetime('now', ? || ' days')
     ORDER BY posted_at DESC`,
    [`-${days}`],
  );
}
