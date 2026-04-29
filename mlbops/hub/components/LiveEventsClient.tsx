"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { getApiBase } from "@/lib/api";

// ── types (mirror FastAPI schemas in mlbops/api/routers/live.py) ─────────────

interface LiveGameSummary {
  game_pk: number;
  game_date: string | null;
  status_abstract: string;
  status_detailed: string;
  away_team_abbr: string | null;
  home_team_abbr: string | null;
  away_score: number | null;
  home_score: number | null;
  inning: number | null;
  inning_state: string | null;
}

interface LiveEvent {
  id: number;
  dedupe_key: string;
  game_pk: number;
  game_date: string;
  event_type: string;
  player_id: number | null;
  player_name: string | null;
  headline: string;
  tweet_text: string;
  payload: Record<string, unknown> | null;
  status: "new" | "queued" | "dismissed";
  queue_id: number | null;
  detected_at: string;
}

interface ScanResponse {
  date: string;
  games_scanned: number;
  games_total: number;
  new_events: number;
  events: LiveEvent[];
  errors: string[];
}

type EventFilter = "new" | "queued" | "dismissed" | "all";

// ── helpers ──────────────────────────────────────────────────────────────────

const EVENT_TYPE_LABEL: Record<string, string> = {
  hr:           "HR",
  multi_hr:     "MULTI-HR",
  no_hit_bid:   "NO-HIT BID",
  k_milestone:  "K MILESTONE",
  cycle_watch:  "CYCLE WATCH",
  final:        "FINAL",
  debut:        "DEBUT",
};

const EVENT_TYPE_CLS: Record<string, string> = {
  hr:           "bg-accent-bg text-accent border-accent/35",
  multi_hr:     "bg-accent-bg-active text-accent border-accent/60",
  no_hit_bid:   "bg-warning-bg text-warning border-warning-border",
  k_milestone:  "bg-info-bg text-info border-info-border",
  cycle_watch:  "bg-success-bg text-success border-success-border",
  final:        "bg-surface-container text-slate-300 border-outline",
  debut:        "bg-success-bg text-success border-success-border",
};

function todayET(): string {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date());
}

function fmtDetectedAt(iso: string): string {
  if (!iso) return "—";
  const parsed = iso.includes("T") ? iso : iso.replace(" ", "T") + "Z";
  try {
    const d = new Date(parsed);
    return d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
  } catch {
    return iso;
  }
}

function gameLabel(g: LiveGameSummary): string {
  const away = g.away_team_abbr || "?";
  const home = g.home_team_abbr || "?";
  const as = g.away_score ?? 0;
  const hs = g.home_score ?? 0;
  const abs = (g.status_abstract || "").toLowerCase();
  if (abs === "preview") return `${away} @ ${home} — ${g.status_detailed || "Scheduled"}`;
  if (abs === "final") return `${away} ${as}, ${home} ${hs} — FINAL`;
  const inn = g.inning ? ` · ${g.inning_state || ""} ${g.inning}` : "";
  return `${away} ${as}, ${home} ${hs}${inn}`;
}

// ── component ────────────────────────────────────────────────────────────────

export default function LiveEventsClient() {
  const api = getApiBase();
  const [date, setDate] = useState<string>(todayET);
  const [events, setEvents] = useState<LiveEvent[]>([]);
  const [games, setGames] = useState<LiveGameSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [filter, setFilter] = useState<EventFilter>("new");
  const [lastScan, setLastScan] = useState<ScanResponse | null>(null);

  const loadEvents = useCallback(async () => {
    setLoading(true);
    setMsg(null);
    try {
      // Load events and games independently so a temporary MLB schedule failure
      // does not block the rest of the Live console.
      const evRes = await fetch(`${api}/live/events?date=${date}`);
      if (!evRes.ok) {
        const body = await evRes.text();
        throw new Error(`events ${evRes.status}${body ? `: ${body}` : ""}`);
      }
      const evs = (await evRes.json()) as LiveEvent[];
      setEvents(evs);

      const gmRes = await fetch(`${api}/live/games?date=${date}`);
      if (!gmRes.ok) {
        const body = await gmRes.text();
        setGames([]);
        setMsg({
          ok: false,
          text: `Live slate unavailable (${gmRes.status}). FastAPI is running, but MLB schedule fetch failed${body ? `: ${body.slice(0, 180)}` : ""}`,
        });
      } else {
        const gms = (await gmRes.json()) as LiveGameSummary[];
        setGames(gms);
      }
    } catch (e) {
      setMsg({
        ok: false,
        text: `Cannot reach ${api}. Start FastAPI on port 8000 (./start_hub.sh). ${(e as Error).message}`,
      });
    } finally {
      setLoading(false);
    }
  }, [api, date]);

  const runScan = useCallback(async () => {
    setScanning(true);
    setMsg(null);
    try {
      const res = await fetch(`${api}/live/scan?date=${date}`, { method: "POST" });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(body || `scan ${res.status}`);
      }
      const data = (await res.json()) as ScanResponse;
      setLastScan(data);
      setEvents(data.events);
      setMsg({
        ok: true,
        text: `Scan ok — ${data.games_scanned}/${data.games_total} games · ${data.new_events} new event${data.new_events === 1 ? "" : "s"}${data.errors.length ? ` · ${data.errors.length} error${data.errors.length === 1 ? "" : "s"}` : ""}.`,
      });
      // games summary is cheap; refresh it too
      const gmRes = await fetch(`${api}/live/games?date=${date}`);
      if (gmRes.ok) setGames((await gmRes.json()) as LiveGameSummary[]);
    } catch (e) {
      setMsg({ ok: false, text: `Scan failed: ${(e as Error).message}` });
    } finally {
      setScanning(false);
    }
  }, [api, date]);

  useEffect(() => {
    loadEvents();
  }, [loadEvents]);

  const queueEvent = async (id: number) => {
    try {
      const res = await fetch(`${api}/live/events/${id}/queue`, { method: "POST" });
      if (!res.ok) throw new Error(`${res.status}`);
      const updated = (await res.json()) as LiveEvent;
      setEvents((prev) => prev.map((e) => (e.id === id ? updated : e)));
      setMsg({
        ok: true,
        text: `Queued event #${id}${updated.queue_id ? ` → queue ${updated.queue_id}` : ""}. Open the Queue to edit and post.`,
      });
    } catch (e) {
      setMsg({ ok: false, text: `Queue failed: ${(e as Error).message}` });
    }
  };

  const dismissEvent = async (id: number) => {
    try {
      const res = await fetch(`${api}/live/events/${id}/dismiss`, { method: "POST" });
      if (!res.ok) throw new Error(`${res.status}`);
      const updated = (await res.json()) as LiveEvent;
      setEvents((prev) => prev.map((e) => (e.id === id ? updated : e)));
    } catch (e) {
      setMsg({ ok: false, text: `Dismiss failed: ${(e as Error).message}` });
    }
  };

  const liveGames = useMemo(
    () =>
      games.filter((g) => {
        const abs = (g.status_abstract || "").toLowerCase();
        return abs === "live" || abs === "final";
      }),
    [games],
  );

  const filteredEvents = useMemo(() => {
    if (filter === "all") return events;
    return events.filter((e) => e.status === filter);
  }, [events, filter]);

  const counts = useMemo(() => {
    const c = { new: 0, queued: 0, dismissed: 0, all: events.length };
    for (const e of events) c[e.status] += 1;
    return c;
  }, [events]);

  return (
    <div className="flex flex-col gap-4 px-6 py-4">
      {/* Toolbar */}
      <div className="flex flex-wrap items-center gap-3">
        <input
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          className="bg-surface border border-outline px-3 py-2 text-sm font-mono text-foreground"
        />
        <button
          onClick={runScan}
          disabled={scanning}
          className="px-4 py-2 bg-accent text-white font-headline uppercase tracking-wider text-sm font-bold hover:bg-accent/90 disabled:opacity-50 transition-colors"
        >
          {scanning ? "Scanning…" : "Scan now"}
        </button>
        <button
          onClick={loadEvents}
          disabled={loading}
          className="px-3 py-2 border border-outline text-slate-300 font-mono uppercase text-xs hover:bg-surface-hover"
        >
          {loading ? "…" : "Reload"}
        </button>
        <span className="text-xs font-mono text-slate-500">
          {lastScan
            ? `Last scan: ${lastScan.games_scanned}/${lastScan.games_total} games · ${lastScan.new_events} new`
            : "Click Scan to detect live events"}
        </span>
      </div>

      {msg && (
        <div
          className={`px-3 py-2 text-sm font-mono border ${
            msg.ok
              ? "bg-success-bg text-success border-success-border"
              : "bg-danger-bg text-danger border-danger-border"
          }`}
        >
          {msg.text}
        </div>
      )}

      {/* Live games strip */}
      <div className="border border-outline bg-surface">
        <div className="px-3 py-2 border-b border-outline text-xs font-mono text-accent-soft uppercase tracking-widest">
          Live slate — {liveGames.length} game{liveGames.length === 1 ? "" : "s"}
        </div>
        {liveGames.length === 0 ? (
          <div className="px-3 py-3 text-sm text-slate-500 font-mono">
            No live or final games for {date}.
          </div>
        ) : (
          <ul className="divide-y divide-outline/40">
            {liveGames.map((g) => {
              const abs = (g.status_abstract || "").toLowerCase();
              const cls =
                abs === "live"
                  ? "text-success"
                  : abs === "final"
                    ? "text-slate-400"
                    : "text-slate-500";
              return (
                <li
                  key={g.game_pk}
                  className="px-3 py-2 flex items-center justify-between gap-3 text-sm font-mono"
                >
                  <span className={cls}>{gameLabel(g)}</span>
                  <span className="text-xs text-slate-500 uppercase">{g.status_detailed}</span>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      {/* Event filter tabs */}
      <div className="flex items-center gap-1 border-b border-outline">
        {(["new", "queued", "dismissed", "all"] as const).map((k) => (
          <button
            key={k}
            onClick={() => setFilter(k)}
            className={`px-3 py-2 text-xs font-headline uppercase tracking-widest border-b-2 transition-colors ${
              filter === k
                ? "text-accent border-accent"
                : "text-slate-400 border-transparent hover:text-foreground"
            }`}
          >
            {k} ({counts[k]})
          </button>
        ))}
      </div>

      {/* Events table */}
      {filteredEvents.length === 0 ? (
        <div className="border border-outline bg-surface p-6 text-center text-sm font-mono text-slate-500">
          {events.length === 0
            ? "No events detected yet for this date. Run a scan when games are live."
            : `No ${filter} events.`}
        </div>
      ) : (
        <div className="border border-outline bg-surface overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-surface-header text-xs font-mono uppercase tracking-wider text-slate-400">
              <tr>
                <th className="px-3 py-2 text-left">Time</th>
                <th className="px-3 py-2 text-left">Type</th>
                <th className="px-3 py-2 text-left">Headline</th>
                <th className="px-3 py-2 text-left">Draft tweet</th>
                <th className="px-3 py-2 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-outline/40">
              {filteredEvents.map((ev) => {
                const labelCls = EVENT_TYPE_CLS[ev.event_type] || "bg-surface-container text-slate-300 border-outline";
                const label = EVENT_TYPE_LABEL[ev.event_type] || ev.event_type.toUpperCase();
                return (
                  <tr key={ev.id} className="align-top">
                    <td className="px-3 py-2 text-xs font-mono text-slate-500 whitespace-nowrap">
                      {fmtDetectedAt(ev.detected_at)}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap">
                      <span
                        className={`inline-block px-2 py-0.5 text-[10px] font-mono uppercase tracking-widest border ${labelCls}`}
                      >
                        {label}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-foreground">
                      <div className="font-medium">{ev.headline}</div>
                      {ev.player_name && (
                        <div className="text-xs text-slate-500 font-mono mt-0.5">
                          {ev.player_name} · game {ev.game_pk}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-2 text-xs font-mono text-slate-300 whitespace-pre-wrap max-w-[520px]">
                      {ev.tweet_text}
                    </td>
                    <td className="px-3 py-2 whitespace-nowrap text-right">
                      {ev.status === "new" && (
                        <div className="inline-flex gap-1">
                          <button
                            onClick={() => queueEvent(ev.id)}
                            className="px-2 py-1 bg-accent text-white text-xs font-headline uppercase tracking-wider hover:bg-accent/90"
                          >
                            Queue
                          </button>
                          <button
                            onClick={() => dismissEvent(ev.id)}
                            className="px-2 py-1 border border-outline text-slate-300 text-xs font-mono uppercase hover:bg-surface-hover"
                          >
                            Dismiss
                          </button>
                        </div>
                      )}
                      {ev.status === "queued" && (
                        <Link
                          href="/queue"
                          className="inline-block px-2 py-1 border border-success-border text-success text-xs font-mono uppercase hover:bg-success-bg"
                        >
                          In queue {ev.queue_id ? `#${ev.queue_id}` : ""}
                        </Link>
                      )}
                      {ev.status === "dismissed" && (
                        <span className="text-xs font-mono text-slate-500 uppercase">Dismissed</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
