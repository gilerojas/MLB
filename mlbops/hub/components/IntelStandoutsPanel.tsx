"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { fetchJson, getApiBase } from "@/lib/api";

type WindowKey = "yesterday" | "7d" | "14d" | "month";

interface PitcherRow {
  player_id: number;
  player_name: string;
  team: string;
  opponent: string;
  game_date: string;
  game_pk: number;
  game_score: number;
  line: string;
  feed_path?: string | null;
  data_source?: string;
}

interface BatterRow {
  player_id: number;
  player_name: string;
  team: string;
  opponent: string;
  game_date: string;
  game_pk: number;
  malli_score: number;
  line: string;
  feed_path?: string | null;
  parquet_path?: string | null;
  data_source?: string;
}

interface StandoutsPayload {
  window: string;
  date_start: string;
  date_end: string;
  as_of: string;
  data_source?: "warehouse" | "api";
  source_note?: string | null;
  api_boxscores_fetched?: number;
  api_schedule_days?: number;
  api_capped?: boolean;
  warehouse_dir?: string;
  raw_dirs_touched?: number;
  feeds_scanned: number;
  feeds_not_final?: number;
  feeds_no_qualifying_lines?: number;
  parse_errors: number;
  hint?: string | null;
  pitchers: PitcherRow[];
  batters: BatterRow[];
}

type GenStatus = "idle" | "generating" | "done" | "error";

interface GenState {
  status: GenStatus;
  error?: string;
  item_id?: number;
}

const WINDOWS: { id: WindowKey; label: string }[] = [
  { id: "yesterday", label: "YESTERDAY" },
  { id: "7d", label: "7D" },
  { id: "14d", label: "14D" },
  { id: "month", label: "30D" },
];

function rowKey(kind: "p" | "b", r: PitcherRow | BatterRow) {
  return `${kind}-${r.player_id}-${r.game_pk}-${r.game_date}`;
}

export function IntelStandoutsPanel() {
  const [window, setWindow] = useState<WindowKey>("yesterday");
  const [data, setData] = useState<StandoutsPayload | null>(null);
  const [loadErr, setLoadErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [gen, setGen] = useState<Record<string, GenState>>({});

  const load = useCallback(async (w: WindowKey) => {
    setLoading(true);
    setLoadErr(null);
    try {
      const json = await fetchJson<StandoutsPayload>(
        `/intel/daily-standouts?window=${encodeURIComponent(w)}&limit=30`
      );
      setData(json);
    } catch (e) {
      setLoadErr(e instanceof Error ? e.message : String(e));
      setData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load(window);
  }, [window, load]);

  async function queuePitcherCard(row: PitcherRow) {
    const key = rowKey("p", row);
    setGen((prev) => ({ ...prev, [key]: { status: "generating" } }));
    const base = getApiBase();
    try {
      const r = await fetch(`${base}/cards/pitcher`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          player_id: row.player_id,
          game_date: row.game_date,
          tweet_text: `${row.player_name} | ${row.game_date} | #Mallitalytics #MLB`,
        }),
        signal: AbortSignal.timeout(180_000),
      });
      const raw = await r.text();
      let body: { id?: number; detail?: string } = {};
      if (raw) {
        try {
          body = JSON.parse(raw) as typeof body;
        } catch {
          body = { detail: raw.slice(0, 280) };
        }
      }
      if (!r.ok) {
        setGen((prev) => ({
          ...prev,
          [key]: { status: "error", error: body.detail || r.statusText },
        }));
        return;
      }
      setGen((prev) => ({
        ...prev,
        [key]: { status: "done", item_id: body.id },
      }));
    } catch (e) {
      setGen((prev) => ({
        ...prev,
        [key]: { status: "error", error: String(e) },
      }));
    }
  }

  async function queueBatterCard(row: BatterRow) {
    const key = rowKey("b", row);
    if (!row.feed_path && !row.parquet_path) {
      setGen((prev) => ({
        ...prev,
        [key]: {
          status: "error",
          error: "Need local feed_live or pitches_enriched parquet (sync warehouse).",
        },
      }));
      return;
    }
    setGen((prev) => ({ ...prev, [key]: { status: "generating" } }));
    const base = getApiBase();
    try {
      const payload: Record<string, unknown> = {
        player_id: row.player_id,
        tweet_text: `${row.player_name} | ${row.game_date} | #Mallitalytics #MLB`,
      };
      if (row.feed_path) payload.feed_path = row.feed_path;
      else if (row.parquet_path) payload.parquet_path = row.parquet_path;
      else payload.game_date = row.game_date;
      const r = await fetch(`${base}/cards/batter`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(180_000),
      });
      const raw = await r.text();
      let body: { id?: number; detail?: string } = {};
      if (raw) {
        try {
          body = JSON.parse(raw) as typeof body;
        } catch {
          body = { detail: raw.slice(0, 280) };
        }
      }
      if (!r.ok) {
        setGen((prev) => ({
          ...prev,
          [key]: { status: "error", error: body.detail || r.statusText },
        }));
        return;
      }
      setGen((prev) => ({
        ...prev,
        [key]: { status: "done", item_id: body.id },
      }));
    } catch (e) {
      setGen((prev) => ({
        ...prev,
        [key]: { status: "error", error: String(e) },
      }));
    }
  }

  return (
    <section className="border border-outline-variant/30 bg-surface overflow-hidden">
      <div className="p-5 border-b border-outline-variant/30">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between mb-4">
          <h3 className="font-headline text-lg font-semibold text-foreground flex items-center gap-2">
            <span className="material-symbols-outlined text-accent text-2xl" aria-hidden>
              radar
            </span>
            Dynamic standouts
          </h3>
          <div className="flex bg-surface-lowest border border-outline-variant shrink-0">
            {WINDOWS.map((w, i) => (
              <button
                key={w.id}
                type="button"
                onClick={() => setWindow(w.id)}
                className={`px-3 py-1 text-xs font-mono uppercase transition-colors ${
                  window === w.id
                    ? "bg-accent text-[#552000] font-bold"
                    : `text-slate-500 hover:text-white ${i > 0 ? "border-l border-outline-variant" : ""}`
                }`}
              >
                {w.label}
              </button>
            ))}
          </div>
        </div>
        <p className="text-xs text-dim leading-snug font-mono">
          Regular season, Final games only. Prefers local{" "}
          <code className="text-dim">…/regular_season/raw</code> feed_live; MLB Stats API fallback. Pitching by
          Game Score; batting by Malli line score.
        </p>
      </div>

      <div className="px-4 py-2 border-b border-outline-variant/30 bg-surface-header/40 text-xs text-dim font-mono flex flex-wrap gap-x-4 gap-y-1 items-baseline">
        {loading ? (
          <span>Loading…</span>
        ) : data ? (
          <>
            <span>
              Range {data.date_start} → {data.date_end}
            </span>
            <span>Feeds {data.feeds_scanned}</span>
            {(data.api_boxscores_fetched ?? 0) > 0 ? (
              <span className="text-info">
                API boxscores {data.api_boxscores_fetched}
                {data.api_capped ? " (capped)" : ""}
              </span>
            ) : null}
            {data.raw_dirs_touched != null ? (
              <span>Raw dirs {data.raw_dirs_touched}</span>
            ) : null}
            {(data.feeds_not_final ?? 0) > 0 ? (
              <span className="text-warning">Not Final {data.feeds_not_final}</span>
            ) : null}
            {data.parse_errors > 0 ? (
              <span className="text-warning">Parse errors {data.parse_errors}</span>
            ) : null}
            {data.warehouse_dir ? (
              <span
                className="truncate max-w-[min(100%,20rem)] text-dim/90"
                title={data.warehouse_dir}
              >
                {data.warehouse_dir}
              </span>
            ) : null}
          </>
        ) : null}
      </div>

      {!loading && data?.source_note ? (
        <div className="mx-4 mt-3 border border-info-border/45 bg-info-bg/30 px-3 py-2 text-sm text-foreground-muted leading-snug">
          <p>{data.source_note}</p>
        </div>
      ) : null}

      {!loading && data?.hint ? (
        <div className="mx-4 mt-3 border border-warning-border/50 bg-warning-bg/25 px-3 py-2 text-sm text-foreground-muted leading-snug">
          <p>{data.hint}</p>
          <p className="mt-1.5 text-dim">
            Sync:{" "}
            <Link href="/settings" className="text-info hover:underline">
              Settings
            </Link>{" "}
            (Drive) or{" "}
            <code className="text-info/90">./scripts/pull_mlbops_from_drive.sh</code> — ingest writes{" "}
            <code className="text-info/90">{"…/raw/game_{pk}_YYYYMMDD_feed_live.json.gz"}</code>.
          </p>
        </div>
      ) : null}

      {loadErr && (
        <p className="mx-4 my-2 text-xs text-red-300 border border-red-900/40 bg-red-950/20 px-2 py-1.5">
          {loadErr}
        </p>
      )}

      {loading ? (
        <p className="px-4 py-8 text-center text-xs text-dim font-mono">Loading standouts…</p>
      ) : (
      <div className="p-4 grid gap-6 lg:grid-cols-2">
        <div className="min-w-0">
          <h4 className="font-mono text-xs uppercase text-slate-500 mb-3 tracking-widest border-b border-outline-variant/20 pb-1 flex items-center justify-between">
            <span>Pitcher standouts</span>
            <span className="text-accent-soft">EV_P01</span>
          </h4>
          {!loading && data && data.pitchers.length === 0 ? (
            <p className="text-xs text-dim">
              {data.hint ? "No pitching rows (see note above)." : "No qualifying lines in this window."}
            </p>
          ) : (
            <div className="overflow-x-auto max-h-[420px] overflow-y-auto overscroll-contain border border-outline-variant/20 bg-surface-header/30">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-surface-header/95 border-b border-outline-variant/30 text-dim uppercase tracking-wide">
                  <tr>
                    <th className="px-2 py-1.5 font-medium">GSc</th>
                    <th className="px-2 py-1.5 font-medium">Player</th>
                    <th className="px-2 py-1.5 font-medium hidden sm:table-cell">Opp</th>
                    <th className="px-2 py-1.5 font-medium">Date</th>
                    <th className="px-2 py-1.5 font-medium">Line</th>
                    <th className="px-2 py-1.5 font-medium w-[4.5rem]">Card</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.pitchers || []).map((row) => {
                    const k = rowKey("p", row);
                    const g = gen[k];
                    return (
                      <tr
                        key={k}
                        className="border-b border-outline-variant/20 hover:bg-surface-hover/50"
                      >
                        <td className="px-2 py-1.5 font-mono tabular-nums text-foreground">
                          {row.game_score}
                        </td>
                        <td className="px-2 py-1.5 text-foreground min-w-[7rem]">
                          <span className="font-medium">{row.player_name}</span>
                          <span className="text-dim ml-1">{row.team}</span>
                        </td>
                        <td className="px-2 py-1.5 text-muted hidden sm:table-cell">
                          {row.opponent}
                        </td>
                        <td className="px-2 py-1.5 font-mono text-muted whitespace-nowrap">
                          {row.game_date}
                        </td>
                        <td className="px-2 py-1.5 text-muted leading-tight max-w-[10rem] sm:max-w-none">
                          {row.line}
                        </td>
                        <td className="px-2 py-1.5 align-top">
                          <div className="flex flex-col gap-0.5 items-start">
                            <button
                              type="button"
                              disabled={g?.status === "generating"}
                              onClick={() => void queuePitcherCard(row)}
                              className="px-1.5 py-0.5 rounded border border-border text-xs font-semibold uppercase tracking-wide text-dim hover:text-accent hover:border-accent/35 disabled:opacity-40"
                            >
                              {g?.status === "generating" ? "…" : "Queue"}
                            </button>
                            {g?.status === "done" && g.item_id != null ? (
                              <Link
                                href="/queue"
                                className="text-xs text-info hover:underline"
                              >
                                #{g.item_id}
                              </Link>
                            ) : null}
                            {g?.status === "error" ? (
                              <span className="text-xs text-red-300 leading-tight max-w-[5rem] break-words">
                                {g.error}
                              </span>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="min-w-0">
          <h4 className="font-mono text-xs uppercase text-slate-500 mb-3 tracking-widest border-b border-outline-variant/20 pb-1 flex items-center justify-between">
            <span>Batter standouts</span>
            <span className="text-accent-soft">EV_B04</span>
          </h4>
          {!loading && data && data.batters.length === 0 ? (
            <p className="text-xs text-dim">
              {data.hint ? "No batting rows (see note above)." : "No qualifying lines in this window."}
            </p>
          ) : (
            <div className="overflow-x-auto max-h-[420px] overflow-y-auto overscroll-contain border border-outline-variant/20 bg-surface-header/30">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-surface-header/95 border-b border-outline-variant/30 text-dim uppercase tracking-wide">
                  <tr>
                    <th className="px-2 py-1.5 font-medium">Malli</th>
                    <th className="px-2 py-1.5 font-medium">Player</th>
                    <th className="px-2 py-1.5 font-medium hidden sm:table-cell">Opp</th>
                    <th className="px-2 py-1.5 font-medium">Date</th>
                    <th className="px-2 py-1.5 font-medium">Line</th>
                    <th className="px-2 py-1.5 font-medium w-[4.5rem]">Card</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.batters || []).map((row) => {
                    const k = rowKey("b", row);
                    const g = gen[k];
                    return (
                      <tr
                        key={k}
                        className="border-b border-outline-variant/20 hover:bg-surface-hover/50"
                      >
                        <td className="px-2 py-1.5 font-mono tabular-nums text-foreground">
                          {row.malli_score}
                        </td>
                        <td className="px-2 py-1.5 text-foreground min-w-[7rem]">
                          <span className="font-medium">{row.player_name}</span>
                          <span className="text-dim ml-1">{row.team}</span>
                        </td>
                        <td className="px-2 py-1.5 text-muted hidden sm:table-cell">
                          {row.opponent}
                        </td>
                        <td className="px-2 py-1.5 font-mono text-muted whitespace-nowrap">
                          {row.game_date}
                        </td>
                        <td className="px-2 py-1.5 text-muted leading-tight max-w-[10rem] sm:max-w-none">
                          {row.line}
                        </td>
                        <td className="px-2 py-1.5 align-top">
                          <div className="flex flex-col gap-0.5 items-start">
                            <button
                              type="button"
                              disabled={g?.status === "generating" || (!row.feed_path && !row.parquet_path)}
                              title={
                                row.feed_path || row.parquet_path
                                  ? undefined
                                  : "Batter cards need feed_live or pitches_enriched on disk (sync warehouse)."
                              }
                              onClick={() => void queueBatterCard(row)}
                              className="px-1.5 py-0.5 rounded border border-border text-xs font-semibold uppercase tracking-wide text-dim hover:text-accent hover:border-accent/35 disabled:opacity-40"
                            >
                              {!row.feed_path && !row.parquet_path
                                ? "—"
                                : g?.status === "generating"
                                  ? "…"
                                  : "Queue"}
                            </button>
                            {g?.status === "done" && g.item_id != null ? (
                              <Link
                                href="/queue"
                                className="text-xs text-info hover:underline"
                              >
                                #{g.item_id}
                              </Link>
                            ) : null}
                            {g?.status === "error" ? (
                              <span className="text-xs text-red-300 leading-tight max-w-[5rem] break-words">
                                {g.error}
                              </span>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
      )}
    </section>
  );
}
