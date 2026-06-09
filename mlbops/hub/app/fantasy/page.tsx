"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { getApiBase, secureFetch } from "@/lib/api";
import { heatColor, PROJ_RANGES } from "@/lib/heatScale";

type StreamerRow = {
  pitcher: string;
  player_id: number | null;
  team: string;
  team_name: string;
  opponent: string;
  opponent_name: string;
  game_date: string;
  game_pk: number | null;
  venue: string | null;
  home_away: "home" | "away";
  probable_status: string;
  pitcher_hand?: string;
  projected_malli_score?: number;
  projected?: {
    ip?: number;
    pitches?: number;
    batters_faced?: number;
    k?: number;
    bb?: number;
    h?: number;
    hr?: number;
    er?: number;
    whip?: number;
    k_pct?: number;
    bb_pct?: number;
    k_minus_bb_pct?: number;
    swstr_pct?: number;
    csw_pct?: number;
    xwoba_allowed?: number;
  };
  stream_score: number;
  k_upside: number;
  ratio_risk: number;
  opponent_k_profile: number;
  opponent_power_risk: number;
  confidence: number;
  league_fit: string;
  note: string;
  factor_scores: Record<string, number>;
  sample: {
    pitcher_batters_faced: number;
    opponent_pa: number;
    opponent_split?: string;
    opponent_split_fallback?: string;
  };
};

type StreamerPayload = {
  game_date: string;
  season: number;
  source: string;
  notes: string[];
  count: number;
  streamers: StreamerRow[];
};

type QueueDraftResponse = {
  id: number;
  title: string;
  tweet_text: string;
  image_url?: string;
};

function todayIso() {
  const d = new Date();
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${d.getFullYear()}-${month}-${day}`;
}

function malliTier(value: number | null | undefined) {
  if (typeof value !== "number") return "Projection";
  if (value >= 60) return "Strong stream";
  if (value >= 48) return "Fringe stream";
  return "Sit / fade";
}

function titleCase(value: string | null | undefined) {
  if (!value) return "-";
  return value.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function fmt(value: number | null | undefined, digits = 1) {
  if (typeof value !== "number" || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

function splitLabel(row: StreamerRow) {
  const split = row.sample?.opponent_split || row.pitcher_hand || "all";
  const fallback = row.sample?.opponent_split_fallback;
  if (fallback === "team_all_hands") return "All hands";
  if (fallback === "league") return "League";
  return `${split}HP`;
}

function StatusBadge({ status }: { status: string }) {
  const isProbable = status === "probable";
  return (
    <span
      className={`inline-flex border px-2 py-0.5 text-[10px] font-mono uppercase tracking-wide ${
        isProbable
          ? "border-success-border bg-success-bg text-success"
          : "border-warning-border bg-warning-bg text-warning"
      }`}
    >
      {titleCase(status)}
    </span>
  );
}

function PropCell({
  label,
  value,
  numeric,
  lo,
  hi,
}: {
  label: string;
  value: string;
  numeric?: number | null;
  lo: number;
  hi: number;
}) {
  const tint = heatColor(numeric, lo, hi);
  return (
    <div
      className="border px-3 py-3 text-center border-outline-variant/40 bg-surface-container/30"
      style={{ borderColor: typeof numeric === "number" ? `${tint}55` : undefined }}
    >
      <p className="text-[10px] font-mono uppercase tracking-wide text-dim">{label}</p>
      <p className="mt-1 text-2xl font-headline font-bold tabular-nums" style={{ color: tint }}>
        {value}
      </p>
    </div>
  );
}

function ProjectionCard({
  row,
  onQueue,
  busy,
}: {
  row: StreamerRow;
  onQueue: (row: StreamerRow) => void;
  busy: boolean;
}) {
  const projected = row.projected || {};
  const rowId = `${row.player_id || row.pitcher}-${row.game_pk || row.game_date}`;

  return (
    <article className="border border-outline-variant/40 bg-surface flex flex-col">
      <div className="border-b border-outline-variant/30 px-4 py-3 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge status={row.probable_status} />
            {row.pitcher_hand && (
              <span className="text-[10px] font-mono uppercase text-dim">{row.pitcher_hand}HP</span>
            )}
            <span className="text-[10px] font-mono uppercase text-dim">{row.team} · {row.home_away}</span>
          </div>
          <h2 className="mt-2 text-lg font-headline font-bold text-foreground truncate">
            {row.pitcher}
            <span className="text-dim font-normal"> vs </span>
            {row.opponent}
          </h2>
          <p className="mt-0.5 text-xs text-dim">
            {splitLabel(row)} · {row.sample.opponent_pa} PA sample
            {row.venue ? ` · ${row.venue}` : ""}
          </p>
        </div>
        <div className="text-right shrink-0">
          <p className="text-[10px] font-mono uppercase tracking-wide text-dim">Proj. MalliScore</p>
          <p
            className="text-4xl font-headline font-bold tabular-nums leading-none"
            style={{ color: heatColor(row.projected_malli_score, PROJ_RANGES.malli.lo, PROJ_RANGES.malli.hi) }}
          >
            {fmt(row.projected_malli_score)}
          </p>
          <p
            className="mt-1 text-[11px] font-mono uppercase"
            style={{ color: heatColor(row.projected_malli_score, PROJ_RANGES.malli.lo, PROJ_RANGES.malli.hi) }}
          >
            {malliTier(row.projected_malli_score)}
          </p>
        </div>
      </div>

      <div className="px-4 py-4 grid grid-cols-5 gap-2">
        <PropCell label="IP" value={fmt(projected.ip)} numeric={projected.ip} lo={PROJ_RANGES.ip.lo} hi={PROJ_RANGES.ip.hi} />
        <PropCell label="K" value={fmt(projected.k)} numeric={projected.k} lo={PROJ_RANGES.k.lo} hi={PROJ_RANGES.k.hi} />
        <PropCell label="BB" value={fmt(projected.bb)} numeric={projected.bb} lo={PROJ_RANGES.bb.lo} hi={PROJ_RANGES.bb.hi} />
        <PropCell label="H" value={fmt(projected.h)} numeric={projected.h} lo={PROJ_RANGES.h.lo} hi={PROJ_RANGES.h.hi} />
        <PropCell label="ER" value={fmt(projected.er)} numeric={projected.er} lo={PROJ_RANGES.er.lo} hi={PROJ_RANGES.er.hi} />
      </div>

      <div className="px-4 pb-3 flex flex-wrap items-center gap-3 text-xs font-mono text-dim">
        <span style={{ color: heatColor(projected.whip, PROJ_RANGES.whip.lo, PROJ_RANGES.whip.hi) }}>
          WHIP {fmt(projected.whip, 2)}
        </span>
        <span>League fit: {row.league_fit}</span>
        <span>Conf. {row.confidence}</span>
      </div>

      <div className="mt-auto border-t border-outline-variant/30 px-4 py-3 flex items-center justify-between gap-3">
        <p className="text-xs text-foreground-muted leading-relaxed line-clamp-2 flex-1">{row.note}</p>
        <button
          type="button"
          disabled={busy}
          onClick={() => onQueue(row)}
          className="shrink-0 border border-accent bg-accent-bg px-4 py-2 text-[11px] font-headline font-bold uppercase tracking-wide text-accent-soft transition-colors hover:bg-accent-bg-active disabled:cursor-not-allowed disabled:opacity-50"
        >
          {busy ? "Sending" : "Queue slip"}
        </button>
      </div>
    </article>
  );
}

export default function FantasyPage() {
  const api = getApiBase();
  const [gameDate, setGameDate] = useState(todayIso());
  const [limit, setLimit] = useState(20);
  const [includeLiveProbables, setIncludeLiveProbables] = useState(false);
  const [data, setData] = useState<StreamerPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [queued, setQueued] = useState<QueueDraftResponse | null>(null);
  const [queueBusyId, setQueueBusyId] = useState<string | null>(null);

  const season = useMemo(() => Number(gameDate.slice(0, 4)) || new Date().getFullYear(), [gameDate]);
  const sortedRows = useMemo(
    () =>
      [...(data?.streamers || [])].sort(
        (a, b) =>
          (b.projected_malli_score ?? b.stream_score ?? 0) -
          (a.projected_malli_score ?? a.stream_score ?? 0),
      ),
    [data],
  );

  const fetchStreamers = useCallback(async () => {
    setLoading(true);
    setError(null);
    setQueued(null);
    try {
      const params = new URLSearchParams({
        game_date: gameDate,
        season: String(season),
        limit: String(limit),
        include_live_probables: String(includeLiveProbables),
      });
      const res = await fetch(`${api}/fantasy/streamers?${params.toString()}`, { cache: "no-store" });
      const body = await res.json();
      if (!res.ok) throw new Error(typeof body.detail === "string" ? body.detail : "Streamer matrix failed.");
      setData(body as StreamerPayload);
    } catch (e) {
      setData(null);
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [api, gameDate, includeLiveProbables, limit, season]);

  useEffect(() => {
    void fetchStreamers();
  }, [fetchStreamers]);

  async function queueStreamer(row: StreamerRow) {
    const busyId = `${row.player_id || row.pitcher}-${row.game_pk || row.game_date}`;
    setQueueBusyId(busyId);
    setError(null);
    setQueued(null);
    try {
      const res = await secureFetch(`${api}/queue/fantasy-streamer-draft`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          pitcher: row.pitcher,
          player_id: row.player_id,
          team: row.team,
          opponent: row.opponent,
          game_date: row.game_date,
          season,
          game_pk: row.game_pk,
          venue: row.venue,
          home_away: row.home_away,
          probable_status: row.probable_status,
          pitcher_hand: row.pitcher_hand,
          projected_malli_score: row.projected_malli_score,
          projected: row.projected || {},
          stream_score: row.stream_score,
          k_upside: row.k_upside,
          ratio_risk: row.ratio_risk,
          opponent_k_profile: row.opponent_k_profile,
          opponent_power_risk: row.opponent_power_risk,
          confidence: row.confidence,
          league_fit: row.league_fit,
          note: row.note,
          factor_scores: row.factor_scores,
        }),
      });
      const body = await res.json();
      if (!res.ok) throw new Error(typeof body.detail === "string" ? body.detail : "Queue draft failed.");
      setQueued(body as QueueDraftResponse);
    } catch (e) {
      setError(String(e));
    } finally {
      setQueueBusyId(null);
    }
  }

  return (
    <div className="p-4 lg:p-6 max-w-[1800px] mx-auto w-full space-y-5">
      <div className="flex flex-col xl:flex-row xl:items-end xl:justify-between gap-4 border-b border-outline pb-4">
        <div>
          <p className="text-xs font-mono uppercase tracking-widest text-accent">Console / Fantasy</p>
          <h1 className="text-2xl font-headline font-bold text-foreground mt-1">Pitching Projections</h1>
          <p className="text-sm text-dim mt-1">
            Projected stat lines ranked by MalliScore — queue a bet-slip card for Launch station.
          </p>
        </div>
        <div className="grid grid-cols-2 sm:flex sm:items-end gap-2">
          <label className="grid gap-1">
            <span className="text-[10px] font-mono uppercase text-dim">Date</span>
            <input
              type="date"
              value={gameDate}
              onChange={(e) => setGameDate(e.target.value)}
              className="border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-sm font-mono"
            />
          </label>
          <label className="grid gap-1">
            <span className="text-[10px] font-mono uppercase text-dim">Rows</span>
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-sm font-mono"
            >
              <option value={10}>10</option>
              <option value={20}>20</option>
              <option value={30}>30</option>
              <option value={50}>50</option>
            </select>
          </label>
          <label className="col-span-2 sm:col-span-1 flex items-center gap-2 border border-outline-variant bg-surface px-3 py-2 text-xs font-mono uppercase text-dim">
            <input
              type="checkbox"
              checked={includeLiveProbables}
              onChange={(e) => setIncludeLiveProbables(e.target.checked)}
              className="accent-[var(--accent)]"
            />
            Live probables
          </label>
          <button
            type="button"
            onClick={() => void fetchStreamers()}
            className="col-span-2 sm:col-span-1 border border-outline-variant bg-surface px-4 py-2 text-xs font-headline font-bold uppercase tracking-wide text-foreground hover:bg-surface-hover"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && <div className="border border-danger-border bg-danger-bg px-4 py-3 text-sm text-danger font-mono">{error}</div>}
      {queued && (
        <div className="border border-success-border bg-success-bg px-4 py-3 text-sm text-success">
          Queued #{queued.id}: {queued.title}.{" "}
          <Link href="/queue" className="font-mono uppercase underline">Open queue</Link>
          {queued.image_url ? " — projection slip attached." : ""}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="border border-outline-variant/40 bg-surface px-3 py-3">
          <p className="text-[10px] font-mono uppercase text-dim tracking-wide">Date</p>
          <p className="mt-1 text-xl font-headline font-bold text-foreground">{data?.game_date || gameDate}</p>
        </div>
        <div className="border border-outline-variant/40 bg-surface px-3 py-3">
          <p className="text-[10px] font-mono uppercase text-dim tracking-wide">Candidates</p>
          <p className="mt-1 text-xl font-headline font-bold text-accent">{data?.count ?? 0}</p>
        </div>
        <div className="border border-outline-variant/40 bg-surface px-3 py-3">
          <p className="text-[10px] font-mono uppercase text-dim tracking-wide">Season</p>
          <p className="mt-1 text-xl font-headline font-bold text-foreground">{season}</p>
        </div>
        <div className="border border-outline-variant/40 bg-surface px-3 py-3">
          <p className="text-[10px] font-mono uppercase text-dim tracking-wide">Mode</p>
          <p className="mt-1 text-xl font-headline font-bold text-foreground">{includeLiveProbables ? "Live" : "Local"}</p>
        </div>
      </div>

      {loading && (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="border border-outline-variant/40 bg-surface h-64 animate-pulse" />
          ))}
        </div>
      )}

      {!loading && data && sortedRows.length === 0 && (
        <div className="border border-outline-variant/40 bg-surface px-4 py-6 text-sm text-dim">No streamer candidates found for this date.</div>
      )}

      {!loading && data && sortedRows.length > 0 && (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {sortedRows.map((row) => {
            const rowId = `${row.player_id || row.pitcher}-${row.game_pk || row.game_date}`;
            return (
              <ProjectionCard
                key={rowId}
                row={row}
                busy={queueBusyId === rowId}
                onQueue={queueStreamer}
              />
            );
          })}
        </div>
      )}
    </div>
  );
}
