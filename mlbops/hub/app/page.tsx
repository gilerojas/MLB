import type { ReactNode } from "react";
import Link from "next/link";
import { MorningIntelRunPanel } from "@/components/MorningIntelRunPanel";
import { PipelineChecklist } from "@/components/PipelineChecklist";
import { getApiBase } from "@/lib/api";

export const dynamic = "force-dynamic";

// ── types ──────────────────────────────────────────────────────────────────

interface GameCompact {
  game_pk: number;
  game_date: string | null;
  status: string | null;
  away_team: string | null;
  away_team_id: number | null;
  away_score: number | null;
  away_wins: number | null;
  away_losses: number | null;
  away_probable: string | null;
  home_team: string | null;
  home_team_id: number | null;
  home_score: number | null;
  home_wins: number | null;
  home_losses: number | null;
  home_probable: string | null;
}

interface BriefingPayload {
  generated_at_utc?: string;
  intel_anchor?: string | null;
  schedule?: {
    today_date: string;
    yesterday_date: string;
    games_today_count: number;
    games_yesterday_count: number;
    games_today?: GameCompact[];
  };
  snapshot?: Record<string, unknown> | null;
  top_anomalies?: Array<Record<string, unknown>>;
  queue?: { draft_count: number; by_status: Record<string, number> };
  data_freshness?: {
    latest_game_date?: string | null;
    latest_parquet_path?: string | null;
    last_drive_sync_utc?: string | null;
    season_progress?: {
      season: number;
      stage: string;
      games_played: number;
      total_games: number;
      percent: number;
      latest_game_date?: string | null;
      source: string;
    };
  };
}

// ── helpers ────────────────────────────────────────────────────────────────

function fmtGameTime(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      timeZone: "America/New_York",
      hour12: true,
    });
  } catch {
    return "—";
  }
}

function statusBadge(status: string | null): { label: string; cls: string } {
  if (!status) return { label: "—", cls: "text-dim" };
  const s = status.toLowerCase();
  if (s.includes("final")) return { label: "FINAL", cls: "text-slate-400" };
  if (s.includes("progress") || s.includes("live")) {
    const raw = status.trim();
    const short =
      raw.length > 14 ? raw.slice(0, 14).toUpperCase() : raw.toUpperCase();
    return { label: short, cls: "text-success font-mono text-xs" };
  }
  if (s.includes("postponed")) return { label: "PPD", cls: "text-warning" };
  if (s.includes("pre") || s.includes("warmup") || s.includes("scheduled"))
    return { label: "PRE-GAME", cls: "text-slate-500 font-mono text-xs" };
  return { label: "SCHEDULED", cls: "text-slate-500 font-mono text-xs" };
}

function teamAbbrev(name: string | null): string {
  if (!name) return "—";
  const parts = name.trim().split(/\s+/);
  const last = parts[parts.length - 1] || name;
  return last.replace(/[^A-Za-z]/g, "").slice(0, 3).toUpperCase() || "—";
}

// MLB team logo via mlbstatic CDN
function teamLogoUrl(teamId: number | null | undefined): string | null {
  if (!teamId) return null;
  return `https://www.mlbstatic.com/team-logos/${teamId}.svg`;
}

// MLB player headshot CDN
function playerHeadshotUrl(playerId: number | string | null | undefined): string | null {
  if (!playerId) return null;
  return `https://img.mlbstatic.com/mlb-photos/image/upload/w_120,q_100/v1/people/${playerId}/headshot/67/current`;
}

// Map team name fragments → MLB team IDs (covers all 30 teams)
const TEAM_NAME_TO_ID: Record<string, number> = {
  "diamondbacks": 109, "d-backs": 109,
  "braves": 144,
  "orioles": 110,
  "red sox": 111,
  "cubs": 112,
  "white sox": 145,
  "reds": 113,
  "guardians": 114,
  "rockies": 115,
  "tigers": 116,
  "astros": 117,
  "royals": 118,
  "angels": 108,
  "dodgers": 119,
  "marlins": 146,
  "brewers": 158,
  "twins": 142,
  "mets": 121,
  "yankees": 147,
  "athletics": 133, "a's": 133,
  "phillies": 143,
  "pirates": 134,
  "padres": 135,
  "giants": 137,
  "mariners": 136,
  "cardinals": 138,
  "rays": 139,
  "rangers": 140,
  "blue jays": 141,
  "nationals": 120,
};

function extractTeamIdFromText(text: string): number | null {
  const lower = text.toLowerCase();
  for (const [fragment, id] of Object.entries(TEAM_NAME_TO_ID)) {
    if (lower.includes(fragment)) return id;
  }
  return null;
}

// ── sub-components ─────────────────────────────────────────────────────────

function KpiTile({
  label,
  value,
  sub,
  accent,
  subClassName,
}: {
  label: string;
  value: ReactNode;
  sub?: string;
  accent?: "gold" | "green" | "blue" | "dim";
  subClassName?: string;
}) {
  const topBar =
    accent === "gold"
      ? "bg-accent"
      : accent === "green"
        ? "bg-success"
        : accent === "blue"
          ? "bg-tertiary-container"
          : "bg-slate-500";
  return (
    <div className="relative overflow-hidden border border-outline-variant/30 bg-surface">
      <div className={`absolute top-0 left-0 w-full h-0.5 ${topBar}`} aria-hidden />
      <div className="p-4">
        <div className="text-xs font-mono text-slate-500 uppercase tracking-widest mb-1">{label}</div>
        <div className="flex justify-between items-end gap-2">
          <span className="text-2xl font-headline font-bold text-foreground tabular-nums">{value}</span>
          {sub && (
            <span
              className={`text-xs font-mono pb-1 text-right truncate max-w-[58%] ${subClassName ?? "text-slate-400"}`}
            >
              {sub}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

function Panel({
  title,
  action,
  children,
}: {
  title: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section>
      <div className="flex items-center gap-4 mb-4">
        <h3 className="font-headline font-bold text-lg uppercase tracking-tighter text-foreground">{title}</h3>
        <div className="h-px bg-outline-variant/30 flex-1" />
        {action}
      </div>
      <div className="border border-outline-variant/20 bg-surface overflow-hidden">{children}</div>
    </section>
  );
}

function TeamLogo({
  teamId,
  abbrev,
  size = 28,
}: {
  teamId: number | null | undefined;
  abbrev: string;
  size?: number;
}) {
  const url = teamLogoUrl(teamId);
  if (url) {
    return (
      <div
        className="shrink-0 flex items-center justify-center border border-outline-variant bg-white/95 dark:bg-white/95"
        style={{ width: size, height: size }}
      >
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          src={url}
          alt={abbrev}
          width={size}
          height={size}
          className="object-contain"
          style={{ width: Math.round(size * 0.85), height: Math.round(size * 0.85) }}
        />
      </div>
    );
  }
  return (
    <div
      className="bg-slate-800 border border-slate-700 flex items-center justify-center font-bold text-xs text-foreground shrink-0"
      style={{ width: size, height: size }}
    >
      {abbrev}
    </div>
  );
}

function GameRow({ g }: { g: GameCompact }) {
  const live =
    g.status?.toLowerCase().includes("progress") || g.status?.toLowerCase().includes("live");
  const final = g.status?.toLowerCase().includes("final");
  const sb = statusBadge(g.status);
  const timeCol = live || final ? fmtGameTime(g.game_date) : fmtGameTime(g.game_date);
  const dotCls = live ? "bg-success" : final ? "bg-slate-500" : "bg-slate-500";

  return (
    <div className="grid grid-cols-12 px-4 py-3 items-center border-b border-outline-variant/10 last:border-0 hover:bg-surface-hover/20 transition-colors">
      <div className="col-span-12 sm:col-span-2 font-mono text-xs text-foreground">{timeCol}</div>
      <div className="col-span-12 sm:col-span-5 flex items-center gap-3 min-w-0 mt-1 sm:mt-0">
        <TeamLogo teamId={g.away_team_id} abbrev={teamAbbrev(g.away_team)} size={24} />
        <span className="text-xs font-mono text-slate-500">@</span>
        <TeamLogo teamId={g.home_team_id} abbrev={teamAbbrev(g.home_team)} size={24} />
        <span className="text-xs font-headline font-medium text-foreground truncate">
          {teamAbbrev(g.away_team)} @ {teamAbbrev(g.home_team)}
        </span>
      </div>
      <div className="col-span-12 sm:col-span-3 flex items-center gap-2 mt-1 sm:mt-0">
        <span className={`w-1.5 h-1.5 shrink-0 ${dotCls}`} aria-hidden />
        <span className={`text-xs font-mono ${sb.cls}`}>{sb.label}</span>
      </div>
      <div className="col-span-12 sm:col-span-2 text-left sm:text-right font-mono text-sm font-bold mt-1 sm:mt-0">
        {live || final ? (
          <span className={final ? "text-slate-500" : "text-foreground"}>
            {g.away_score ?? 0} - {g.home_score ?? 0}
          </span>
        ) : (
          <span className="text-slate-600">- - -</span>
        )}
      </div>
    </div>
  );
}

function PipelineRow({
  icon,
  iconClass,
  label,
  health,
  dotClass,
  dimmed,
}: {
  icon: string;
  iconClass?: string;
  label: string;
  health: string;
  dotClass: string;
  dimmed?: boolean;
}) {
  return (
    <div
      className={`flex items-center justify-between py-2 border-b border-outline-variant/20 last:border-0 ${
        dimmed ? "opacity-50" : ""
      }`}
    >
      <div className="flex items-center gap-3 min-w-0">
        <span
          className={`material-symbols-outlined text-xl shrink-0 ${iconClass ?? "text-tertiary"}`}
          aria-hidden
        >
          {icon}
        </span>
        <span className="text-xs font-headline font-medium uppercase text-foreground truncate">{label}</span>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <span className="text-xs font-mono text-slate-500 uppercase">{health}</span>
        <span className={`w-2 h-2 shrink-0 ${dotClass}`} aria-hidden />
      </div>
    </div>
  );
}

// ── page ───────────────────────────────────────────────────────────────────

export default async function DashboardPage() {
  const base = getApiBase();
  let data: BriefingPayload | null = null;
  let error: string | null = null;

  try {
    const res = await fetch(`${base}/briefing`, {
      cache: "no-store",
      signal: AbortSignal.timeout(60_000),
    });
    if (!res.ok) error = await res.text();
    else data = (await res.json()) as BriefingPayload;
  } catch (e) {
    error = String(e);
  }

  const snap = data?.snapshot;
  const txs = (snap?.transactions as string[]) || [];
  const games = data?.schedule?.games_today ?? [];
  const liveGames = games.filter(
    (g) => g.status?.toLowerCase().includes("progress") || g.status?.toLowerCase().includes("live")
  );
  const finalGames = games.filter((g) => g.status?.toLowerCase().includes("final"));
  const scheduledGames = games.filter(
    (g) => !g.status?.toLowerCase().includes("progress") &&
           !g.status?.toLowerCase().includes("live") &&
           !g.status?.toLowerCase().includes("final")
  );
  // Sort: live first, then scheduled, then final
  const sortedGames = [...liveGames, ...scheduledGames, ...finalGames];

  const freshness = data?.data_freshness;
  const warehouseDate = freshness?.latest_game_date;
  const lastSync = freshness?.last_drive_sync_utc;
  const intelAnchor = data?.intel_anchor;
  const today = data?.schedule?.today_date ?? new Date().toISOString().slice(0, 10);
  const queueCounts = data?.queue?.by_status ?? {};

  const warehouseFresh =
    warehouseDate != null ? warehouseDate >= today.slice(0, 10) : null;
  const intelFresh =
    intelAnchor != null
      ? intelAnchor >= new Date(Date.now() - 2 * 86400_000).toISOString().slice(0, 10)
      : null;

  const sysTime =
    data?.generated_at_utc != null
      ? `${data.generated_at_utc.slice(0, 10)} ${data.generated_at_utc.slice(11, 19)} UTC`
      : "—";
  const statusLine = error ? "● DEGRADED" : "● NOMINAL";
  const statusCls = error ? "text-danger" : "text-success";
  const draftN = data?.queue?.draft_count ?? 0;
  const seasonProgress = freshness?.season_progress;
  const seasonPct =
    seasonProgress != null ? seasonProgress.percent.toFixed(2) : "—";
  const seasonProgressDetail =
    seasonProgress != null
      ? `${seasonProgress.games_played.toLocaleString()} / ${seasonProgress.total_games.toLocaleString()} games`
      : "warehouse unavailable";

  return (
    <div className="p-6 max-w-[1800px] mx-auto px-8 2xl:px-12">
      {/* Dashboard header */}
      <div className="flex flex-col gap-4 lg:flex-row lg:justify-between lg:items-end mb-8">
        <div>
          <h2 className="font-headline text-4xl font-extrabold tracking-tighter text-foreground">Dashboard</h2>
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-accent-soft font-mono text-xs mt-1">
            <span className="opacity-50">SYS_TIME:</span>
            <span>{sysTime}</span>
            <span className="ml-4 opacity-50">STATUS:</span>
            <span className={statusCls}>{statusLine}</span>
          </div>
          {error && (
            <p className="mt-2 border border-danger/35 bg-danger-bg px-3 py-2 text-xs text-danger">
              API error: {error.slice(0, 160)} — is FastAPI running?
            </p>
          )}
        </div>
        <div className="bg-surface-header border border-outline/20 p-3 flex items-center gap-4 shrink-0">
          <div className="text-right">
            <div className="text-xs text-outline font-mono uppercase tracking-widest leading-none">
              Season Played
            </div>
            <div className="text-xl font-headline font-bold text-foreground">{seasonPct}%</div>
            <div className="text-[10px] font-mono uppercase text-dim">{seasonProgressDetail}</div>
          </div>
          <div className="w-12 h-12 border-2 border-accent/20 flex items-center justify-center">
            <span className="material-symbols-outlined text-accent text-2xl" aria-hidden>
              sports_baseball
            </span>
          </div>
        </div>
      </div>

      <PipelineChecklist />

      {/* KPI row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <KpiTile
          label="Games Today"
          value={data?.schedule?.games_today_count ?? "—"}
          sub={
            liveGames.length
              ? `${liveGames.length} IN_PROGRESS`
              : scheduledGames.length
                ? `${scheduledGames.length} UPCOMING`
                : "COMPLETE"
          }
          accent="gold"
          subClassName={liveGames.length ? "text-success" : "text-slate-400"}
        />
        <KpiTile
          label="Yesterday's Games"
          value={data?.schedule?.games_yesterday_count ?? "—"}
          sub="COMPLETE"
          accent="dim"
        />
        <KpiTile
          label="Queue Drafts"
          value={data?.queue?.draft_count ?? "—"}
          sub={draftN > 0 ? "◆ PENDING_REV" : "CLEAR"}
          accent="gold"
          subClassName={draftN > 0 ? "text-warning" : "text-slate-400"}
        />
        <KpiTile
          label="Warehouse Freshness"
          value={warehouseDate ?? "—"}
          sub={warehouseFresh === true ? "LATEST_SYNC" : "STALE_OR_UNKNOWN"}
          accent="blue"
          subClassName={warehouseFresh === true ? "text-tertiary" : "text-slate-400"}
        />
      </div>

      <div className="mb-8">
        <MorningIntelRunPanel variant="dashboard" />
      </div>

      <div className="grid grid-cols-12 gap-8">
        <div className="col-span-12 lg:col-span-7 space-y-8">
          <Panel
            title={`Today's Games (${games.length})`}
            action={
              <Link href="/schedule" className="text-xs font-mono text-accent hover:underline shrink-0">
                Full schedule
              </Link>
            }
          >
            {sortedGames.length === 0 ? (
              <p className="text-sm text-dim p-4">No games scheduled.</p>
            ) : (
              <>
                <div className="grid grid-cols-12 px-4 py-2 border-b border-outline-variant/20 font-mono text-xs text-slate-500 uppercase">
                  <div className="col-span-12 sm:col-span-2">Time</div>
                  <div className="col-span-12 sm:col-span-5 hidden sm:block">Matchup</div>
                  <div className="col-span-12 sm:col-span-3">Status</div>
                  <div className="col-span-12 sm:col-span-2 text-right">Score</div>
                </div>
                <div className="divide-y divide-outline-variant/10">
                  {sortedGames.map((g) => (
                    <GameRow key={g.game_pk} g={g} />
                  ))}
                </div>
              </>
            )}
          </Panel>

          <section>
            <div className="flex items-center gap-4 mb-4">
              <h3 className="font-headline font-bold text-lg uppercase tracking-tighter text-foreground">
                Hitters to Watch
              </h3>
              <div className="h-px bg-outline-variant/30 flex-1" />
              <span className="material-symbols-outlined text-accent text-sm" aria-hidden>
                warning
              </span>
            </div>
            {(data?.top_anomalies ?? []).length === 0 ? (
              <p className="text-sm text-dim border border-outline-variant/20 bg-surface p-4">
                No snapshot — run morning intel or sync.
              </p>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {(data?.top_anomalies ?? []).slice(0, 6).map((a, i) => {
                  const name =
                    (a.player_name as string) ||
                    (a.name as string) ||
                    (a.pitcher as string) ||
                    (a.batter as string) ||
                    "Player";
                  const metric = String(a.metric ?? "");
                  const delta = a.delta;
                  const deltaNum =
                    typeof delta === "number"
                      ? delta
                      : Number(delta);
                  const hasDeltaNum = Number.isFinite(deltaNum);
                  const deltaUp = hasDeltaNum && deltaNum > 0;
                  const deltaDown = hasDeltaNum && deltaNum < 0;
                  const deltaLabel = hasDeltaNum
                    ? `${deltaUp ? "▲" : deltaDown ? "▼" : "◆"}${Math.abs(deltaNum)}`
                    : `Δ${String(delta)}`;
                  const deltaClass = deltaDown
                    ? "text-danger"
                    : deltaUp
                      ? "text-success"
                      : "text-accent-soft";
                  const windowLabel =
                    typeof a.window_label === "string" && a.window_label.trim()
                      ? (a.window_label as string).trim()
                      : null;
                  const w = a.window_days;
                  const kind = String((a._kind as string) || "FLAG").toUpperCase().slice(0, 12);
                  const playerId = (a.player_id ?? a.mlbam_id ?? a.id) as number | string | null | undefined;
                  const headshotUrl = playerHeadshotUrl(playerId);
                  return (
                    <div
                      key={i}
                      className="bg-surface-header border-l-4 border-accent overflow-hidden"
                    >
                      <div className="flex gap-3 items-stretch">
                        {headshotUrl && (
                          <div className="w-16 shrink-0 bg-slate-900 overflow-hidden self-stretch flex items-end">
                            {/* eslint-disable-next-line @next/next/no-img-element */}
                            <img
                              src={headshotUrl}
                              alt={name}
                              width={64}
                              height={72}
                              className="w-full object-cover object-top"
                            />
                          </div>
                        )}
                        <div className="flex-1 p-3 min-w-0">
                          <div className="flex justify-between items-start mb-2 gap-2">
                            <span className="font-headline font-bold text-sm text-foreground truncate">{name}</span>
                            <span className="font-mono text-xs bg-accent-bg text-accent-soft px-1 shrink-0">
                              {kind}
                            </span>
                          </div>
                          <div className="flex justify-between items-end gap-2">
                            <div className="text-xs font-mono text-slate-500 uppercase truncate">
                              {metric.replace(/_/g, " ")}
                            </div>
                            <div className={`text-xl font-mono font-bold shrink-0 ${deltaClass}`}>
                              {deltaLabel}
                            </div>
                          </div>
                          {(windowLabel || w != null) && (
                            <div className="mt-2 text-xs font-mono text-slate-400 truncate">
                              {windowLabel
                                ? `WINDOW: ${windowLabel}`
                                : `WINDOW: ${String(w)}d`}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
            <div className="mt-3">
              <Link href="/intel" className="text-xs font-mono text-tertiary hover:underline">
                Full intel feed
              </Link>
            </div>
          </section>
        </div>

        <div className="col-span-12 lg:col-span-5 space-y-8">
          <Panel title="System Pipeline">
            <div className="p-4 space-y-1 border-t-0">
              <PipelineRow
                icon="database"
                iconClass="text-tertiary"
                label="Warehouse"
                health={warehouseFresh === true ? "HEALTH: 99%" : "CHECK"}
                dotClass={warehouseFresh === true ? "bg-success" : "bg-warning"}
              />
              <PipelineRow
                icon="psychology"
                iconClass="text-accent"
                label="Intel Engine"
                health={intelFresh ? "HEALTH: NOMINAL" : "STALE"}
                dotClass={intelFresh ? "bg-success" : "bg-slate-500"}
              />
              <PipelineRow
                icon="sync"
                iconClass="text-slate-400"
                label="External Sync"
                health={lastSync ? `SYNCED` : "WAITING"}
                dotClass={lastSync ? "bg-success" : "bg-slate-500"}
                dimmed={!lastSync}
              />
              <PipelineRow
                icon="low_priority"
                iconClass="text-warning"
                label="Queue Worker"
                health={`BUSY: ${draftN} TASKS`}
                dotClass={draftN > 0 ? "bg-warning" : "bg-success"}
              />
            </div>
          </Panel>

          <Panel
            title="Queue"
            action={
              <Link href="/queue" className="text-xs font-mono text-accent hover:underline">
                Review
              </Link>
            }
          >
            <div className="p-4 space-y-2 border-t-0">
              {Object.entries(queueCounts).length === 0 ? (
                <p className="text-sm text-dim">No items.</p>
              ) : (
                Object.entries(queueCounts).map(([status, count]) => (
                  <div key={status} className="flex items-center justify-between font-mono text-xs">
                    <span className="uppercase text-muted">{status}</span>
                    <span
                      className={`tabular-nums font-bold ${
                        status === "draft"
                          ? "text-accent"
                          : status === "posted"
                            ? "text-success"
                            : status === "rejected" || status === "failed"
                              ? "text-danger"
                              : "text-foreground"
                      }`}
                    >
                      {count}
                    </span>
                  </div>
                ))
              )}
            </div>
          </Panel>

          <Panel title="Recent Roster Moves">
            <div className="p-0 border-t-0">
              {txs.length === 0 ? (
                <p className="text-sm text-dim p-4">
                  {snap ? "None in snapshot." : "No intel snapshot loaded."}
                </p>
              ) : (
                <>
                  <div className="space-y-2 p-4">
                    {txs.slice(0, 8).map((t, i) => {
                      const teamId = extractTeamIdFromText(t);
                      const logoUrl = teamLogoUrl(teamId);
                      return (
                        <div
                          key={i}
                          className="flex items-center gap-3 border-l-2 border-r-2 border-outline-variant/20 bg-surface-header/40 px-3 py-2.5 hover:border-accent transition-all group"
                        >
                          {logoUrl ? (
                            // eslint-disable-next-line @next/next/no-img-element
                            <img
                              src={logoUrl}
                              alt="team"
                              width={28}
                              height={28}
                              className="shrink-0 object-contain opacity-80 group-hover:opacity-100"
                              style={{ width: 28, height: 28 }}
                            />
                          ) : (
                            <span className="material-symbols-outlined text-slate-600 text-base shrink-0" aria-hidden>
                              swap_horiz
                            </span>
                          )}
                          <p className="text-xs font-mono text-slate-300 leading-relaxed group-hover:text-foreground flex-1">
                            {t}
                          </p>
                        </div>
                      );
                    })}
                  </div>
                  {txs.length > 8 && (
                    <p className="text-xs text-dim px-4 pb-4">+{txs.length - 8} more — see Intel</p>
                  )}
                  <Link
                    href="/intel"
                    className="block w-full mt-0 bg-surface-header border-t border-outline-variant/30 py-2 font-headline text-xs font-bold uppercase tracking-widest text-center text-foreground hover:bg-accent hover:text-[#552000] transition-all"
                  >
                    View Intel Feed
                  </Link>
                </>
              )}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
