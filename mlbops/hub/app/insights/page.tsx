"use client";

import { useCallback, useEffect, useState } from "react";
import { getApiBase } from "@/lib/api";
import { InsightQueueButton } from "@/components/InsightQueueButton";

// ── types ─────────────────────────────────────────────────────────────────────

interface LeaderRow {
  player_name?: string;
  player_id?: number;
  [key: string]: unknown;
}

interface StatcastBundle {
  player_name: string;
  player_id: number;
  [key: string]: unknown;
}

interface StatcastThresholds {
  team_games?: number;
  bip?: number;
  bbe?: number;
  pitches?: number;
  breaking_pitches?: number;
  tracked_swings?: number;
  pitch_type_pitches?: number;
}

interface StatcastResponse {
  season: number;
  stage: string;
  n_pitches_total: number;
  pitcher_role?: string;
  pitcher_role_filter_supported?: boolean;
  sample_thresholds?: StatcastThresholds;
  bundles: {
    fastball_whiff: StatcastBundle[];
    hardest_throwers: StatcastBundle[];
    pitcher_luck: StatcastBundle[];
    exit_velocity: StatcastBundle[];
    barrel_leaders: StatcastBundle[];
    farthest_home_runs: StatcastBundle[];
    spin_rate: StatcastBundle[];
    chase_kings: StatcastBundle[];
    bs75_leaders: StatcastBundle[];
    pitch_rv100_best: StatcastBundle[];
    pitch_rv100_worst: StatcastBundle[];
    batter_xwoba: StatcastBundle[];
    batter_luck: StatcastBundle[];
  };
}

type TileStatus = "idle" | "loading" | "ok" | "error";
interface TileState {
  status: TileStatus;
  rows: LeaderRow[];
  error?: string;
  min_pa?: number;
  min_ip?: number;
  qualification?: {
    team_games?: number;
    min_pa?: number;
    min_ip?: number;
    batting_rule?: string;
    pitching_rule?: string;
  } | null;
}

type PitcherRoleFilter = "all" | "starter" | "reliever";

function rowThresholds(rows?: StatcastBundle[]): StatcastThresholds | undefined {
  const meta = rows?.find((row) => row._sample_thresholds)?.["_sample_thresholds"];
  return meta && typeof meta === "object" ? (meta as StatcastThresholds) : undefined;
}

// ── formatters ────────────────────────────────────────────────────────────────

const f = (v: unknown, d = 1) => {
  if (v == null) return "—";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toFixed(d);
};
const pct = (v: unknown) => (v == null ? "—" : `${f(v, 1)}%`);
const mph = (v: unknown) => (v == null ? "—" : `${f(v, 1)}`);
const rpm = (v: unknown) => {
  if (v == null) return "—";
  const n = Number(v);
  return isNaN(n) ? "—" : `${Math.round(n)}`;
};

function isoTodayLocal(): string {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function formatAsOfLabel(iso: string): string {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
  if (!m) return iso;
  const dt = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  return dt.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function buildLeaderInsightTweet(
  label: string,
  season: number,
  asOfLabel: string,
  rows: LeaderRow[],
  statKey: string,
  format: (v: unknown) => string,
  sublabel: string,
  limit = 8,
): string {
  const lines = rows.slice(0, limit).map((r, i) => {
    const name = String(r.player_name ?? `ID ${r.player_id}`);
    return `${i + 1}. ${name} — ${format(r[statKey])}`;
  });
  return `${label} · ${season} season (as of ${asOfLabel})\n\n${lines.join("\n")}\n\n${sublabel}\n\n#Mallitalytics`;
}

function serializableLeaderRows(rows: LeaderRow[], statKey: string, limit: number) {
  return rows.slice(0, limit).map((r) => ({
    player_name: r.player_name ?? null,
    player_id: r.player_id ?? null,
    team_abbrev: r.team_abbrev ?? null,
    team_id: r.team_id ?? null,
    [statKey]: r[statKey],
  }));
}

function buildScInsightTweet(
  label: string,
  season: number,
  asOfLabel: string,
  rows: StatcastBundle[],
  statKey: string,
  format: (v: unknown) => string,
  sublabel: string,
  limit = 8,
): string {
  const lines = rows.slice(0, limit).map((r, i) => {
    const name = r.player_name ?? `ID ${r.player_id}`;
    return `${i + 1}. ${name} — ${format(r[statKey])}`;
  });
  return `${label} · ${season} season (as of ${asOfLabel})\n\n${lines.join("\n")}\n\n${sublabel}\n\n#Mallitalytics`;
}

function serializableScRows(rows: StatcastBundle[], statKey: string, limit: number) {
  return rows.slice(0, limit).map((r) => {
    const base: Record<string, unknown> = {
      player_name: r.player_name,
      player_id: r.player_id,
      [statKey]: r[statKey],
    };
    const extra = [
      "pitch_type",
      "pitch_name",
      "n_pitches",
      "avg_velo",
      "n_bip",
      "n_tracked_swings",
      "n_fast_swings",
      "launch_speed",
      "launch_angle",
      "game_date",
      "game_pk",
      "pitcher_name",
      "pitcher_id",
      "description",
    ] as const;
    for (const k of extra) {
      if (r[k] != null) base[k] = r[k];
    }
    return base;
  });
}

function buildBatterLuckTweet(
  kind: "lucky" | "unlucky",
  season: number,
  asOfLabel: string,
  rows: StatcastBundle[],
  limit = 8,
): string {
  const title = kind === "lucky" ? "Lucky hitters (wOBA > xwOBA on BIP)" : "Unlucky hitters (xwOBA > wOBA on BIP)";
  const lines = rows.slice(0, limit).map((r, i) => {
    const delta = Number(r.luck_delta);
    const sign = delta > 0 ? "+" : "";
    const name = r.player_name ?? `ID ${r.player_id}`;
    return `${i + 1}. ${name} — ${sign}${delta.toFixed(3)} (xwOBA ${Number(r.xwoba_bip).toFixed(3)} · wOBA ${Number(r.woba_bip).toFixed(3)})`;
  });
  const sub =
    kind === "lucky"
      ? "Outcomes on balls in play ahead of contact quality"
      : "Contact quality ahead of outcomes on balls in play";
  return `${title} · ${season} season (as of ${asOfLabel})\n\n${lines.join("\n")}\n\n${sub}\n\n#Mallitalytics`;
}

function buildLuckTweet(
  kind: "lucky" | "unlucky",
  season: number,
  asOfLabel: string,
  rows: StatcastBundle[],
  limit = 8,
): string {
  const title = kind === "lucky" ? "Lucky pitchers" : "Unlucky pitchers";
  const lines = rows.slice(0, limit).map((r, i) => {
    const delta = Number(r.luck_delta);
    const sign = delta > 0 ? "+" : "";
    const name = r.player_name ?? `ID ${r.player_id}`;
    return `${i + 1}. ${name} — ${sign}${delta.toFixed(3)} (xwOBA ${Number(r.xwoba_allowed).toFixed(3)} · wOBA ${Number(r.woba_allowed).toFixed(3)})`;
  });
  const sub =
    kind === "lucky"
      ? "xwOBA allowed > wOBA (results better than contact)"
      : "wOBA allowed > xwOBA (results worse than contact)";
  return `${title} · ${season} season (as of ${asOfLabel})\n\n${lines.join("\n")}\n\n${sub}\n\n#Mallitalytics`;
}

// ── skeleton ──────────────────────────────────────────────────────────────────

function Skeleton({ rows = 8 }: { rows?: number }) {
  return (
    <div className="animate-pulse space-y-0">
      {Array.from({ length: rows }).map((_, i) => (
        <div
          key={i}
          className="flex items-center gap-3 px-4 py-2.5 border-b border-border-dim"
        >
          <div className="w-5 h-3 rounded bg-border" />
          <div className="flex-1 h-3 rounded bg-border" />
          <div className="w-10 h-3 rounded bg-border" />
        </div>
      ))}
    </div>
  );
}

// ── tile wrapper ──────────────────────────────────────────────────────────────

function Tile({
  label,
  sublabel,
  accent,
  children,
  meta,
  headerAction,
}: {
  label: string;
  sublabel: string;
  accent: string;
  children: React.ReactNode;
  meta?: string;
  headerAction?: React.ReactNode;
}) {
  return (
    <div className="flex flex-col rounded-lg border border-border bg-surface overflow-hidden">
      <div className={`px-4 py-3 border-b border-border border-l-2 ${accent} bg-surface-header`}>
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <p className="text-sm font-semibold tracking-tight text-foreground">{label}</p>
            <p className="text-sm text-dim mt-0.5 leading-snug">{sublabel}</p>
          </div>
          {headerAction}
        </div>
      </div>
      <div className="flex-1">{children}</div>
      {meta && (
        <div className="px-4 py-1.5 border-t border-border-dim">
          <p className="text-xs text-dim">{meta}</p>
        </div>
      )}
    </div>
  );
}

function TileRow({
  rank,
  name,
  value,
  sub,
  valueColor,
}: {
  rank: number;
  name: string;
  value: string;
  sub?: string;
  valueColor?: string;
}) {
  return (
    <div className="flex items-center gap-2.5 px-4 py-2 border-b border-border-dim hover:bg-surface-hover last:border-0">
      <span className="text-sm font-mono text-dim w-4 text-right shrink-0">
        {rank}
      </span>
      <div className="flex-1 min-w-0">
        <p className="text-sm text-foreground-muted truncate leading-tight">{name}</p>
        {sub && <p className="text-xs text-dim leading-tight mt-0.5">{sub}</p>}
      </div>
      <span
        className={`text-sm font-semibold tabular-nums shrink-0 ${valueColor ?? "text-foreground"}`}
      >
        {value}
      </span>
    </div>
  );
}

// ── section header ────────────────────────────────────────────────────────────

function Section({ label, tag }: { label: string; tag: string }) {
  return (
    <div className="flex items-center gap-3 mt-8 mb-3">
      <span className="text-xs font-semibold uppercase tracking-[0.12em] text-dim">
        {tag}
      </span>
      <div className="flex-1 h-px bg-border" />
      <span className="text-xs font-mono text-dim">{label}</span>
    </div>
  );
}

// ── leaderboard tile ──────────────────────────────────────────────────────────

function LeaderTile({
  state,
  label,
  sublabel,
  accent,
  statKey,
  format,
  valueColor,
  season,
  insightKey,
  pitcherRole,
}: {
  state: TileState;
  label: string;
  sublabel: string;
  accent: string;
  statKey: string;
  format: (v: unknown) => string;
  valueColor?: string;
  season: number;
  insightKey: string;
  pitcherRole?: PitcherRoleFilter;
}) {
  return (
    <Tile
      label={label}
      sublabel={sublabel}
      accent={accent}
      headerAction={
        <InsightQueueButton
          disabled={state.status !== "ok" || state.rows.length === 0}
          buildPayload={() => {
            const asOf = isoTodayLocal();
            const asOfLabel = formatAsOfLabel(asOf);
            return {
              title: `${label} · ${season} · ${asOf}`,
              tweet_text: buildLeaderInsightTweet(
                label,
                season,
                asOfLabel,
                state.rows,
                statKey,
                format,
                sublabel,
              ),
              game_date: asOf,
              season,
              meta: {
                insight_key: insightKey,
                label,
                sublabel,
                stat_key: statKey,
                pitcher_role: pitcherRole ?? null,
                qualification: state.qualification ?? null,
                rows: serializableLeaderRows(state.rows, statKey, 12),
              },
            };
          }}
        />
      }
    >
      {state.status === "loading" || state.status === "idle" ? (
        <Skeleton rows={6} />
      ) : state.status === "error" ? (
        <p className="px-4 py-6 text-xs text-red-400">{state.error}</p>
      ) : state.rows.length === 0 ? (
        <p className="px-4 py-6 text-xs text-dim">No data for this season / filter.</p>
      ) : (
        state.rows.map((r, i) => (
          <TileRow
            key={i}
            rank={i + 1}
            name={String(r.player_name ?? `ID ${r.player_id}`)}
            value={format(r[statKey])}
            valueColor={valueColor}
          />
        ))
      )}
    </Tile>
  );
}

// ── statcast tile ─────────────────────────────────────────────────────────────

function ScTile({
  rows,
  loading,
  error,
  label,
  sublabel,
  accent,
  statKey,
  format,
  sub,
  valueColor,
  season,
  insightKey,
  pitcherRole,
}: {
  rows: StatcastBundle[];
  loading: boolean;
  error: string | null;
  label: string;
  sublabel: string;
  accent: string;
  statKey: string;
  format: (v: unknown) => string;
  sub?: (r: StatcastBundle) => string;
  valueColor?: string;
  season: number;
  insightKey: string;
  pitcherRole: PitcherRoleFilter;
}) {
  return (
    <Tile
      label={label}
      sublabel={sublabel}
      accent={accent}
      headerAction={
        <InsightQueueButton
          disabled={loading || !!error || rows.length === 0}
          buildPayload={() => {
            const asOf = isoTodayLocal();
            const asOfLabel = formatAsOfLabel(asOf);
            return {
              title: `${label} · ${season} · ${asOf}`,
              tweet_text: buildScInsightTweet(
                label,
                season,
                asOfLabel,
                rows,
                statKey,
                format,
                sublabel,
              ),
              game_date: asOf,
              season,
              meta: {
                insight_key: insightKey,
                label,
                sublabel,
                stat_key: statKey,
                pitcher_role: pitcherRole,
                rows: serializableScRows(rows, statKey, 12),
              },
            };
          }}
        />
      }
    >
      {loading ? (
        <Skeleton rows={6} />
      ) : error ? (
        <p className="px-4 py-6 text-sm text-amber-400/80 leading-relaxed">{error}</p>
      ) : rows.length === 0 ? (
        <p className="px-4 py-6 text-xs text-dim">No qualifying players yet.</p>
      ) : (
        rows.slice(0, 8).map((r, i) => (
          <TileRow
            key={i}
            rank={i + 1}
            name={r.player_name ?? `ID ${r.player_id}`}
            value={format(r[statKey])}
            sub={sub ? sub(r) : undefined}
            valueColor={valueColor}
          />
        ))
      )}
    </Tile>
  );
}

// ── luck tile ─────────────────────────────────────────────────────────────────

function LuckRow({
  rank,
  name,
  delta,
  xwoba,
  woba,
  bip,
  lucky,
}: {
  rank: number;
  name: string;
  delta: number;
  xwoba: number;
  woba: number;
  bip: number;
  lucky: boolean;
}) {
  return (
    <div className="flex items-center gap-2.5 px-4 py-2 border-b border-border-dim hover:bg-surface-hover last:border-0">
      <span className="text-sm font-mono text-dim w-4 text-right shrink-0">{rank}</span>
      <div className="flex-1 min-w-0">
        <p className="text-sm text-foreground-muted truncate leading-tight">{name}</p>
        <p className="text-xs text-dim leading-tight mt-0.5">
          xwOBA {xwoba.toFixed(3)} · wOBA {woba.toFixed(3)} · {bip} BIP
        </p>
      </div>
      <span
        className={`text-sm font-semibold tabular-nums shrink-0 ${lucky ? "text-emerald-400" : "text-rose-400"}`}
      >
        {lucky ? "+" : ""}
        {delta.toFixed(3)}
      </span>
    </div>
  );
}

function BatterLuckSection({
  rows,
  loading,
  error,
  season,
}: {
  rows: StatcastBundle[];
  loading: boolean;
  error: string | null;
  season: number;
}) {
  const lucky = rows.filter((r) => Number(r.luck_delta) > 0).slice(0, 8);
  const unlucky = rows
    .filter((r) => Number(r.luck_delta) < 0)
    .slice(0, 8)
    .reverse();

  const body = (items: StatcastBundle[], isLucky: boolean) => {
    if (loading) return <Skeleton rows={5} />;
    if (error)
      return (
        <p className="px-4 py-6 text-sm text-amber-400/80 leading-relaxed">{error}</p>
      );
    if (items.length === 0)
      return <p className="px-4 py-6 text-xs text-dim">No qualifying hitters yet.</p>;
    return items.map((r, i) => (
      <LuckRow
        key={i}
        rank={i + 1}
        name={r.player_name ?? `ID ${r.player_id}`}
        delta={Number(r.luck_delta)}
        xwoba={Number(r.xwoba_bip)}
        woba={Number(r.woba_bip)}
        bip={Number(r.n_bip)}
        lucky={isLucky}
      />
    ));
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <Tile
        label="Lucky hitters"
        sublabel="wOBA on BIP ahead of xwOBA — outcomes beating contact"
        accent="border-emerald-700"
        headerAction={
          <InsightQueueButton
            disabled={loading || !!error || lucky.length === 0}
            buildPayload={() => {
              const asOf = isoTodayLocal();
              const asOfLabel = formatAsOfLabel(asOf);
              return {
                title: `Lucky hitters · ${season} · ${asOf}`,
                tweet_text: buildBatterLuckTweet("lucky", season, asOfLabel, lucky),
                game_date: asOf,
                season,
                meta: {
                  insight_key: "batter_luck_lucky",
                  label: "Lucky hitters",
                  sublabel: "wOBA on BIP ahead of xwOBA — outcomes beating contact",
                  stat_key: "luck_delta",
                  rows: lucky.slice(0, 12).map((r) => ({
                    player_name: r.player_name,
                    player_id: r.player_id,
                    luck_delta: r.luck_delta,
                    xwoba_bip: r.xwoba_bip,
                    woba_bip: r.woba_bip,
                    n_bip: r.n_bip,
                  })),
                },
              };
            }}
          />
        }
      >
        {body(lucky, true)}
      </Tile>
      <Tile
        label="Unlucky hitters"
        sublabel="xwOBA on BIP ahead of wOBA — loud contact, quiet box score"
        accent="border-rose-700"
        headerAction={
          <InsightQueueButton
            disabled={loading || !!error || unlucky.length === 0}
            buildPayload={() => {
              const asOf = isoTodayLocal();
              const asOfLabel = formatAsOfLabel(asOf);
              return {
                title: `Unlucky hitters · ${season} · ${asOf}`,
                tweet_text: buildBatterLuckTweet("unlucky", season, asOfLabel, unlucky),
                game_date: asOf,
                season,
                meta: {
                  insight_key: "batter_luck_unlucky",
                  label: "Unlucky hitters",
                  sublabel: "xwOBA on BIP ahead of wOBA — loud contact, quiet box score",
                  stat_key: "luck_delta",
                  rows: unlucky.slice(0, 12).map((r) => ({
                    player_name: r.player_name,
                    player_id: r.player_id,
                    luck_delta: r.luck_delta,
                    xwoba_bip: r.xwoba_bip,
                    woba_bip: r.woba_bip,
                    n_bip: r.n_bip,
                  })),
                },
              };
            }}
          />
        }
      >
        {body(unlucky, false)}
      </Tile>
    </div>
  );
}

function LuckSection({
  rows,
  loading,
  error,
  season,
  pitcherRole,
}: {
  rows: StatcastBundle[];
  loading: boolean;
  error: string | null;
  season: number;
  pitcherRole: PitcherRoleFilter;
}) {
  const lucky = rows.filter((r) => Number(r.luck_delta) > 0).slice(0, 8);
  const unlucky = rows
    .filter((r) => Number(r.luck_delta) < 0)
    .slice(0, 8)
    .reverse();

  const body = (items: StatcastBundle[], isLucky: boolean) => {
    if (loading) return <Skeleton rows={5} />;
    if (error)
      return (
        <p className="px-4 py-6 text-sm text-amber-400/80 leading-relaxed">{error}</p>
      );
    if (items.length === 0)
      return <p className="px-4 py-6 text-xs text-dim">No qualifying pitchers yet.</p>;
    return items.map((r, i) => (
      <LuckRow
        key={i}
        rank={i + 1}
        name={r.player_name ?? `ID ${r.player_id}`}
        delta={Number(r.luck_delta)}
        xwoba={Number(r.xwoba_allowed)}
        woba={Number(r.woba_allowed)}
        bip={Number(r.n_bip)}
        lucky={isLucky}
      />
    ));
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <Tile
        label="Lucky pitchers"
        sublabel="xwOBA allowed > wOBA — results better than contact quality"
        accent="border-emerald-700"
        headerAction={
          <InsightQueueButton
            disabled={loading || !!error || lucky.length === 0}
            buildPayload={() => {
              const asOf = isoTodayLocal();
              const asOfLabel = formatAsOfLabel(asOf);
              return {
                title: `Lucky pitchers · ${season} · ${asOf}`,
                tweet_text: buildLuckTweet("lucky", season, asOfLabel, lucky),
                game_date: asOf,
                season,
                meta: {
                  insight_key: "pitcher_luck_lucky",
                  label: "Lucky pitchers",
                  sublabel: "xwOBA allowed > wOBA — results better than contact quality",
                  stat_key: "luck_delta",
                  pitcher_role: pitcherRole,
                  rows: lucky.slice(0, 12).map((r) => ({
                    player_name: r.player_name,
                    player_id: r.player_id,
                    luck_delta: r.luck_delta,
                    xwoba_allowed: r.xwoba_allowed,
                    woba_allowed: r.woba_allowed,
                    n_bip: r.n_bip,
                  })),
                },
              };
            }}
          />
        }
      >
        {body(lucky, true)}
      </Tile>
      <Tile
        label="Unlucky pitchers"
        sublabel="wOBA allowed > xwOBA — results worse than contact quality"
        accent="border-rose-700"
        headerAction={
          <InsightQueueButton
            disabled={loading || !!error || unlucky.length === 0}
            buildPayload={() => {
              const asOf = isoTodayLocal();
              const asOfLabel = formatAsOfLabel(asOf);
              return {
                title: `Unlucky pitchers · ${season} · ${asOf}`,
                tweet_text: buildLuckTweet("unlucky", season, asOfLabel, unlucky),
                game_date: asOf,
                season,
                meta: {
                  insight_key: "pitcher_luck_unlucky",
                  label: "Unlucky pitchers",
                  sublabel: "wOBA allowed > xwOBA — results worse than contact quality",
                  stat_key: "luck_delta",
                  pitcher_role: pitcherRole,
                  rows: unlucky.slice(0, 12).map((r) => ({
                    player_name: r.player_name,
                    player_id: r.player_id,
                    luck_delta: r.luck_delta,
                    xwoba_allowed: r.xwoba_allowed,
                    woba_allowed: r.woba_allowed,
                    n_bip: r.n_bip,
                  })),
                },
              };
            }}
          />
        }
      >
        {body(unlucky, false)}
      </Tile>
    </div>
  );
}

// ── page ──────────────────────────────────────────────────────────────────────

export default function InsightsPage() {
  const api = getApiBase();
  const [season, setSeason] = useState(2026);
  const [pitcherRole, setPitcherRole] = useState<PitcherRoleFilter>("all");
  const [pitchingRoleNote, setPitchingRoleNote] = useState<string | null>(null);
  /** False when warehouse source has no games / games_started (SP/RP labels hidden). */
  const [pitchingFilterSupported, setPitchingFilterSupported] = useState<boolean | null>(null);

  const [hrState, setHrState] = useState<TileState>({ status: "idle", rows: [] });
  const [opsState, setOpsState] = useState<TileState>({ status: "idle", rows: [] });
  const [kState, setKState] = useState<TileState>({ status: "idle", rows: [] });
  const [eraState, setEraState] = useState<TileState>({ status: "idle", rows: [] });

  const [sc, setSc] = useState<StatcastResponse | null>(null);
  const [scLoading, setScLoading] = useState(false);
  const [scError, setScError] = useState<string | null>(null);
  const [scRequested, setScRequested] = useState(false);

  const fetchLeader = useCallback(
    async (
      kind: "batting" | "pitching",
      sortBy: string,
      ascending: boolean,
      minPa: number,
      minIp: number,
      limit: number,
      set: (s: TileState) => void,
      role?: PitcherRoleFilter,
      qualified = false,
    ) => {
      set({ status: "loading", rows: [] });
      try {
        const roleParam =
          kind === "pitching" && role && role !== "all"
            ? `&pitcher_role=${encodeURIComponent(role)}`
            : "";
        const url =
          kind === "batting"
            ? `${api}/leaderboards/batting?season=${season}&sort_by=${sortBy}&min_pa=${minPa}&limit=${limit}&qualified=${qualified}`
            : `${api}/leaderboards/pitching?season=${season}&sort_by=${sortBy}&min_ip=${minIp}&limit=${limit}&ascending=${ascending}${roleParam}&qualified=${qualified}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(60_000) });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail ?? "Failed");
        if (kind === "pitching") {
          setPitchingRoleNote(
            typeof data.pitcher_role_note === "string" ? data.pitcher_role_note : null,
          );
          setPitchingFilterSupported(data.pitcher_role_filter_supported === true);
        }
        set({
          status: "ok",
          rows: data.leaders ?? [],
          min_pa: typeof data.min_pa === "number" ? data.min_pa : undefined,
          min_ip: typeof data.min_ip === "number" ? data.min_ip : undefined,
          qualification: data.qualification ?? null,
        });
      } catch (e) {
        set({ status: "error", rows: [], error: String(e) });
      }
    },
    [api, season],
  );

  const fetchSc = useCallback(async () => {
    setScRequested(true);
    setScLoading(true);
    setScError(null);
    try {
      const roleQ = `&pitcher_role=${encodeURIComponent(pitcherRole)}`;
      const res = await fetch(`${api}/insights/statcast?season=${season}${roleQ}`, {
        signal: AbortSignal.timeout(120_000),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail ?? "Statcast fetch failed");
      setSc(data);
    } catch (e) {
      setScError(String(e));
    } finally {
      setScLoading(false);
    }
  }, [api, season, pitcherRole]);

  useEffect(() => {
    void fetchSc();
  }, [fetchSc]);

  useEffect(() => {
    let active = true;

    void (async () => {
      await fetchLeader("batting", "hr", false, 1, 0, 10, setHrState);
      if (!active) return;
      await fetchLeader("batting", "ops", false, 0, 0, 10, setOpsState, undefined, true);
      if (!active) return;
      await fetchLeader("pitching", "strikeouts", false, 0, 0, 10, setKState, pitcherRole);
      if (!active) return;
      await fetchLeader("pitching", "era", true, 0, 0, 10, setEraState, pitcherRole, true);
    })();

    return () => {
      active = false;
    };
  }, [fetchLeader, pitcherRole]);

  const pitchSublabel = (base: string) => {
    if (pitchingFilterSupported !== true) return base;
    if (pitcherRole === "starter") return `${base} · starters (more GS than relief apps)`;
    if (pitcherRole === "reliever") return `${base} · relievers (more relief apps than GS)`;
    return base;
  };
  const b = sc?.bundles;
  const statcastTileError =
    scRequested || scLoading || sc ? scError : "Statcast not loaded yet.";
  const st =
    sc?.sample_thresholds ??
    rowThresholds(b?.barrel_leaders) ??
    rowThresholds(b?.fastball_whiff) ??
    rowThresholds(b?.bs75_leaders) ??
    rowThresholds(b?.pitch_rv100_best);
  const opsSublabel =
    typeof opsState.min_pa === "number" && opsState.min_pa > 0
      ? `OBP + SLG · qualified (${opsState.min_pa} PA min)`
      : typeof st?.team_games === "number" && st.team_games > 0
        ? `OBP + SLG · qualified (${Math.ceil(st.team_games * 3.1)} PA min)`
      : "OBP + SLG · qualified";
  const eraSublabel =
    typeof eraState.min_ip === "number" && eraState.min_ip > 0
      ? pitchSublabel(`Lowest ERA · qualified (${eraState.min_ip.toFixed(0)} IP min)`)
      : typeof st?.team_games === "number" && st.team_games > 0
        ? pitchSublabel(`Lowest ERA · qualified (${st.team_games.toFixed(0)} IP min)`)
      : pitchSublabel("Lowest ERA · qualified");

  const bipMin = st?.bip ?? 8;
  const bbeMin = st?.bbe ?? 25;
  const pitchMin = st?.pitches ?? 20;
  const breakingPitchMin = st?.breaking_pitches ?? 10;
  const trackedSwingMin = st?.tracked_swings ?? 40;
  const pitchTypeMin = st?.pitch_type_pitches ?? 150;

  return (
    <div className="p-6 max-w-[1800px] mx-auto px-8 2xl:px-12">
      {/* Page header */}
      <div className="flex flex-wrap items-end justify-between gap-4 mb-1">
        <div>
          <h1 className="text-2xl font-headline font-bold text-foreground tracking-tight">Insights</h1>
          <p className="text-sm text-dim mt-0.5">
            Live leaders and Statcast analytics.
            {sc && (
              <span className="ml-2 text-dim">
                {sc.n_pitches_total.toLocaleString()} pitches · {sc.season}
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => void fetchSc()}
            disabled={scLoading}
            className="rounded border border-border bg-surface px-3 py-1.5 text-sm font-medium text-muted transition-colors hover:bg-surface-hover hover:text-foreground disabled:cursor-wait disabled:opacity-60"
          >
            {scLoading ? "Loading Statcast..." : sc ? "Refresh Statcast" : "Load Statcast"}
          </button>
          <div className="flex rounded border border-border overflow-hidden">
            {([2024, 2025, 2026] as const).map((y) => (
              <button
                key={y}
                type="button"
                onClick={() => {
                  setSeason(y);
                  setSc(null);
                  setScError(null);
                  setScRequested(false);
                }}
                className={`px-4 py-1.5 text-sm font-medium transition-colors ${
                  season === y
                    ? "bg-info text-white"
                    : "bg-surface text-dim hover:text-foreground-muted"
                }`}
              >
                {y}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Statcast unavailable banner */}
      {scError && !sc && (
        <div className="mt-4 rounded border border-amber-900/40 bg-amber-950/10 px-4 py-3 text-sm text-amber-400/80">
          Statcast tiles unavailable — run daily ingest to populate pitches_enriched parquets.
          <span className="ml-2 text-dim">{scError}</span>
        </div>
      )}

      {/* ═══ Batting (boxscore + batter Statcast) ═══════════════════════════ */}
      <header className="mt-8 mb-1">
        <h2 className="text-sm font-semibold text-foreground tracking-tight">Batting</h2>
        <p className="text-sm text-dim mt-0.5">
          Season boxscore leaders, then contact-quality Statcast (same season as header).
        </p>
      </header>

      <Section tag="&#9654; Boxscore" label="HR · OPS" />
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <LeaderTile
          state={hrState}
          label="Home run leaders"
          sublabel="HR hit this season"
          accent="border-rose-700"
          statKey="hr"
          format={(v) => String(v ?? "—")}
          season={season}
          insightKey="home_run_leaders"
        />
        <LeaderTile
          state={opsState}
          label="OPS leaders"
          sublabel={opsSublabel}
          accent="border-sky-700"
          statKey="ops"
          format={(v) => f(v, 3)}
          season={season}
          insightKey="ops_leaders"
        />
      </div>

      <Section tag="&#9679; Statcast" label="contact quality" />
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">
        <ScTile
          rows={b?.farthest_home_runs ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Farthest home runs"
          sublabel="Longest HR distance this season"
          accent="border-red-600"
          statKey="hit_distance"
          format={(v) => (v == null ? "—" : `${Math.round(Number(v))} ft`)}
          sub={(r) => {
            const parts = [];
            if (r.launch_speed != null) parts.push(`${mph(r.launch_speed)} mph`);
            if (r.launch_angle != null) parts.push(`${f(r.launch_angle, 0)}° LA`);
            if (r.game_date != null) parts.push(String(r.game_date));
            return parts.join(" · ");
          }}
          season={season}
          insightKey="farthest_home_runs"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.barrel_leaders ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Barrel %"
          sublabel={`Barrels per BIP · qualified (${bipMin} BIP min)`}
          accent="border-rose-600"
          statKey="barrel_pct"
          format={pct}
          sub={(r) => `${r.barrels} barrels / ${r.n_bip} BIP`}
          season={season}
          insightKey="barrel_pct"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.exit_velocity ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Exit velocity"
          sublabel={`Avg EV on contact · qualified (${bipMin} BIP min)`}
          accent="border-orange-600"
          statKey="avg_ev"
          format={(v) => `${mph(v)} mph`}
          sub={(r) => `Max ${mph(r.max_ev)} mph`}
          season={season}
          insightKey="exit_velocity"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.batter_xwoba ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="xwOBA on contact"
          sublabel={`Mean xwOBA on BIP · qualified (${bbeMin} BBE min)`}
          accent="border-sky-600"
          statKey="xwoba"
          format={(v) => f(v, 3)}
          sub={(r) => `${r.n_bip} BIP`}
          season={season}
          insightKey="batter_xwoba"
          pitcherRole={pitcherRole}
        />
      </div>
      <p className="mt-2 text-xs text-dim max-w-3xl">
        Batter luck compares mean wOBA vs mean xwOBA on balls in play (qualified BBE floor).
      </p>
      <div className="mt-4">
        <BatterLuckSection
          rows={b?.batter_luck ?? []}
          loading={scLoading}
          error={statcastTileError}
          season={season}
        />
      </div>

      {/* ═══ Pitching (boxscore + pitcher Statcast) ═══════════════════════════ */}
      <header className="mt-12 pt-8 border-t border-border">
        <h2 className="text-sm font-semibold text-foreground tracking-tight">Pitching</h2>
        <p className="text-sm text-dim mt-0.5">
          Boxscore K / ERA, pitch-mix Statcast, and luck — one All / SP / RP control for every
          pitcher-facing view below (same season as header).
        </p>
      </header>

      <div className="mt-4 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border bg-surface-header px-3 py-2.5">
        <p className="text-sm text-muted leading-snug max-w-[min(100%,32rem)]">
          Pitcher filter · applies to K, ERA, arsenal tiles, and luck (Statcast rows restricted to
          matching MLBAM pitcher IDs).
        </p>
        <div className="flex rounded border border-border overflow-hidden shrink-0">
          {(
            [
              ["all", "All"],
              ["starter", "SP"],
              ["reliever", "RP"],
            ] as const
          ).map(([val, label]) => (
            <button
              key={val}
              type="button"
              title={
                val === "starter"
                  ? "More starts than relief appearances"
                  : val === "reliever"
                    ? "More relief appearances than starts"
                    : "All pitchers"
              }
              onClick={() => {
                setPitcherRole(val);
                setSc(null);
                setScError(null);
                setScRequested(false);
              }}
              className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                pitcherRole === val
                  ? "bg-surface-hover text-foreground border-x border-border first:border-l-0 last:border-r-0"
                  : "bg-surface text-dim hover:text-foreground-muted"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      {pitchingRoleNote && (
        <p className="mt-2 text-xs text-amber-400/90 leading-snug max-w-4xl">{pitchingRoleNote}</p>
      )}

      {/* Boxscore: K / ERA */}
      <div className="mt-6">
        <div className="flex items-center gap-3 mb-3">
          <span className="text-xs font-semibold uppercase tracking-[0.12em] text-dim">
            &#9654; Boxscore
          </span>
          <div className="flex-1 h-px bg-border" />
          <span className="text-xs font-mono text-dim">K · ERA</span>
        </div>
        <div className="rounded-lg border border-border bg-surface overflow-hidden">
          <div className="px-3 py-2 border-b border-border bg-surface-header">
            <p className="text-sm text-muted">Season totals from warehouse / Stats-style exports</p>
          </div>
          <div className="p-3 grid grid-cols-1 sm:grid-cols-2 gap-3">
            <LeaderTile
              state={kState}
              label="Strikeout leaders"
              sublabel={pitchSublabel("Total K thrown this season")}
              accent="border-violet-700"
              statKey="strikeouts"
              format={(v) => String(v ?? "—")}
              season={season}
              insightKey="strikeout_leaders"
              pitcherRole={pitcherRole}
            />
            <LeaderTile
              state={eraState}
              label="ERA leaders"
              sublabel={eraSublabel}
              accent="border-teal-700"
              statKey="era"
              format={(v) => f(v, 2)}
              season={season}
              insightKey="era_leaders"
              pitcherRole={pitcherRole}
            />
          </div>
        </div>
      </div>

      <Section tag="&#9670; Statcast" label="pitching arsenal" />
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3">
        <ScTile
          rows={b?.fastball_whiff ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Fastball whiff %"
          sublabel={pitchSublabel(`FF / SI swing-and-miss rate · qualified (${pitchMin} pitches min)`)}
          accent="border-sky-600"
          statKey="whiff_pct"
          format={pct}
          sub={(r) => `${r.n_pitches} pitches`}
          season={season}
          insightKey="fastball_whiff"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.hardest_throwers ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Hardest throwers"
          sublabel={pitchSublabel(`Avg FF / SI velocity · qualified (${pitchMin} pitches min)`)}
          accent="border-amber-600"
          statKey="avg_velo"
          format={(v) => `${mph(v)} mph`}
          sub={(r) => `Max ${mph(r.max_velo)} mph`}
          season={season}
          insightKey="hardest_throwers"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.chase_kings ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Chase rate"
          sublabel={pitchSublabel(`Out-of-zone swing % induced · qualified (${pitchMin} OOZ pitches min)`)}
          accent="border-teal-600"
          statKey="chase_pct"
          format={pct}
          sub={(r) => `${r.n_pitches} OOZ pitches`}
          season={season}
          insightKey="chase_rate"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.spin_rate ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Spin rate"
          sublabel={pitchSublabel(`Highest avg spin · breaking balls (${breakingPitchMin} pitch min)`)}
          accent="border-purple-600"
          statKey="avg_spin"
          format={(v) => `${rpm(v)} rpm`}
          sub={(r) => String(r.pitch_type)}
          season={season}
          insightKey="spin_rate"
          pitcherRole={pitcherRole}
        />
      </div>

      <div className="mt-3 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        <ScTile
          rows={b?.bs75_leaders ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="BS75+% (tracked swings)"
          sublabel={pitchSublabel(`Share of swings ≥ 75 mph bat speed · qualified (${trackedSwingMin} tracked min)`)}
          accent="border-lime-600"
          statKey="bs75_pct"
          format={pct}
          sub={(r) => `${r.n_fast_swings} fast / ${r.n_tracked_swings} tracked`}
          season={season}
          insightKey="bs75_leaders"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.pitch_rv100_best ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Best pitches (RV/100)"
          sublabel={pitchSublabel(`Highest run value per 100 pitches · qualified (${pitchTypeMin} per type)`)}
          accent="border-emerald-600"
          statKey="rv100"
          format={(v) => (v == null ? "—" : `${Number(v).toFixed(1)} RV/100`)}
          sub={(r) =>
            `${String(r.pitch_type)}${r.pitch_name ? ` · ${String(r.pitch_name)}` : ""} · avg ${mph(r.avg_velo)} mph`
          }
          season={season}
          insightKey="pitch_rv100_best"
          pitcherRole={pitcherRole}
        />
        <ScTile
          rows={b?.pitch_rv100_worst ?? []}
          loading={scLoading}
          error={statcastTileError}
          label="Worst pitches (RV/100)"
          sublabel={pitchSublabel(`Lowest RV/100 · qualified (${pitchTypeMin} per type)`)}
          accent="border-rose-600"
          statKey="rv100"
          format={(v) => (v == null ? "—" : `${Number(v).toFixed(1)} RV/100`)}
          sub={(r) =>
            `${String(r.pitch_type)}${r.pitch_name ? ` · ${String(r.pitch_name)}` : ""} · avg ${mph(r.avg_velo)} mph`
          }
          season={season}
          insightKey="pitch_rv100_worst"
          pitcherRole={pitcherRole}
        />
      </div>
      <p className="mt-2 text-xs text-dim max-w-3xl">
        BS75+% uses tracked swings only (swing outcomes with non-null bat speed). RV/100 uses
        Statcast delta_run_exp (−sum / n × 100); higher is better for the pitcher.
      </p>

      <Section
        tag="&#8597; Luck &amp; regression"
        label={pitchSublabel("xwOBA vs wOBA · pitchers")}
      />
      <LuckSection
        rows={b?.pitcher_luck ?? []}
        loading={scLoading}
        error={statcastTileError}
        season={season}
        pitcherRole={pitcherRole}
      />
    </div>
  );
}
