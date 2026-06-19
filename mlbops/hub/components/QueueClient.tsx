"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { QueueItem } from "@/lib/db";
import { getApiBase, secureFetch } from "@/lib/api";

const STATUS_TABS = ["draft", "all", "approved", "posted", "rejected", "failed"] as const;
type StatusTab = (typeof STATUS_TABS)[number];

const SORT_OPTIONS = [
  { value: "created_at:desc", label: "Newest" },
  { value: "created_at:asc", label: "Oldest" },
  { value: "priority_score:desc", label: "Priority ↓" },
  { value: "game_date:desc", label: "Game date ↓" },
  { value: "game_date:asc", label: "Game date ↑" },
] as const;

const DEFAULT_TAXONOMY = {
  content_pillars: ["probables", "pitcher_to_watch", "player_card", "leaderboard_watch", "statcast_signal", "pitching_index", "hr_tracker", "buy_sell", "matchup_edge", "live_event", "text_only"],
  hook_types: ["hidden_edge", "what_changed", "one_chart_one_takeaway", "signal_vs_noise", "box_score_missed", "bookmark_utility", "debate_prompt", "rare_air", "live_reaction"],
  intended_kpis: ["bookmarks", "replies", "reposts", "profile_visits", "follows", "impressions"],
};

const STATUS_COLORS: Record<string, string> = {
  draft:    "bg-accent-bg-active text-accent border border-accent/35",
  approved: "bg-info-bg text-info border border-info-border",
  posted:   "bg-success-bg text-success border border-success-border",
  rejected: "bg-danger-bg text-danger border border-danger-border",
  failed:   "bg-warning-bg text-warning border border-warning-border",
};

function apiDownMessage(apiBase: string): string {
  return `Cannot reach the MLB Ops API through ${apiBase}. Start the full hub from the MLB repo root with ./scripts/start_mlbops.sh, or use ./scripts/start_mlbops_travel.sh before Tailscale Serve.`;
}

const CONTENT_TYPE_LABEL: Record<string, string> = {
  batter_card:      "Batter card",
  pitcher_card:     "Pitcher card",
  hr_tracker:       "HR Tracker",
  pitching_index:   "Pitching Index",
  games_of_day:     "Games of Day",
  probables_board:  "Probables board",
  insight_tile:     "Insight",
  text_only:         "Text post",
  live_event:       "Live event",
  fantasy_streamer: "Pitching projection",
};

function CharCounter({ text, max }: { text: string; max: number }) {
  const len = text.length;
  const warn = Math.max(0, Math.floor(max * 0.9));
  const color =
    len > max ? "text-danger font-bold" : len > warn ? "text-warning" : "text-dim";
  return (
    <span className={`text-xs tabular-nums ${color}`}>
      {len}/{max.toLocaleString()}
    </span>
  );
}

type StreakStats = {
  posted_today: number;
  weekly_total: number;
  current_streak: number;
  longest_streak: number;
  manual_ratio: number;
};

type PerformanceMetrics = {
  x_post_id?: string | null;
  impressions: number;
  likes: number;
  replies: number;
  reposts: number;
  quote_tweets: number;
  bookmarks: number;
  profile_visits: number;
  follows: number;
  notes?: string | null;
  engagement_rate?: number;
  bookmark_rate?: number;
  reply_rate?: number;
  repost_rate?: number;
  follows_per_1000_impressions?: number;
};

type Taxonomy = typeof DEFAULT_TAXONOMY;

type ContentScore = {
  priority_score: number;
  primary_kpi: string;
  recommended_pillar: string;
  reason: string;
  factors: Record<string, number>;
  scored_at?: string;
  model?: string;
};

const EMPTY_PERFORMANCE: PerformanceMetrics = {
  impressions: 0,
  likes: 0,
  replies: 0,
  reposts: 0,
  quote_tweets: 0,
  bookmarks: 0,
  profile_visits: 0,
  follows: 0,
  notes: "",
};

function parseMeta(raw: string | null): Record<string, unknown> | null {
  if (!raw) return null;
  try { return JSON.parse(raw) as Record<string, unknown>; } catch { return null; }
}

function titleCase(value: string | null | undefined) {
  if (!value) return "—";
  return value.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function MetadataEditor({
  item,
  taxonomy,
  onSave,
}: {
  item: QueueItem;
  taxonomy: Taxonomy;
  onSave: (patch: Record<string, string | number>) => Promise<void>;
}) {
  const editable = item.status === "draft";
  const selectClass = "mt-1 w-full border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-xs font-mono";
  const inputClass = "mt-1 w-full border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-xs font-mono";
  if (!editable) {
    const cells = [
      ["Pillar", titleCase(item.content_pillar)],
      ["Hook", titleCase(item.hook_type)],
      ["Primary KPI", titleCase(item.intended_kpi)],
      ["Priority", item.priority_score == null ? "—" : String(item.priority_score)],
      ["Campaign", item.campaign || "—"],
      ["Source", item.source_module || "—"],
      ["Mode", titleCase(item.manual_or_ai)],
      ["Experiment tag", item.experiment_tag || "—"],
    ];
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
        {cells.map(([label, value]) => (
          <div key={label} className="border border-outline-variant/40 bg-surface-lowest px-3 py-2">
            <p className="text-[10px] font-mono uppercase text-dim">{label}</p>
            <p className="text-xs font-mono text-foreground truncate">{value}</p>
          </div>
        ))}
      </div>
    );
  }
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Pillar
        <select className={selectClass} value={item.content_pillar || ""} onChange={(e) => onSave({ content_pillar: e.target.value })}>
          {taxonomy.content_pillars.map((v) => <option key={v} value={v}>{titleCase(v)}</option>)}
        </select>
      </label>
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Hook
        <select className={selectClass} value={item.hook_type || ""} onChange={(e) => onSave({ hook_type: e.target.value })}>
          {taxonomy.hook_types.map((v) => <option key={v} value={v}>{titleCase(v)}</option>)}
        </select>
      </label>
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Primary KPI
        <select className={selectClass} value={item.intended_kpi || ""} onChange={(e) => onSave({ intended_kpi: e.target.value })}>
          {taxonomy.intended_kpis.map((v) => <option key={v} value={v}>{titleCase(v)}</option>)}
        </select>
      </label>
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Priority
        <input className={inputClass} type="number" min={0} max={100} value={item.priority_score ?? 0} onChange={(e) => onSave({ priority_score: Number(e.target.value || 0) })} />
      </label>
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Campaign
        <input className={inputClass} value={item.campaign || ""} onChange={(e) => onSave({ campaign: e.target.value })} />
      </label>
      <div className="border border-outline-variant/40 bg-surface-lowest px-3 py-2">
        <p className="text-[10px] font-mono uppercase text-dim">Source</p>
        <p className="text-xs font-mono text-foreground truncate mt-2">{item.source_module || "—"}</p>
      </div>
      <div className="border border-outline-variant/40 bg-surface-lowest px-3 py-2">
        <p className="text-[10px] font-mono uppercase text-dim">Mode</p>
        <p className="text-xs font-mono text-foreground truncate mt-2">{titleCase(item.manual_or_ai)}</p>
      </div>
      <label className="border border-outline-variant/40 bg-surface-lowest px-3 py-2 text-[10px] font-mono uppercase text-dim">
        Experiment tag
        <input className={inputClass} value={item.experiment_tag || ""} onChange={(e) => onSave({ experiment_tag: e.target.value })} />
      </label>
    </div>
  );
}

function ScorePanel({ score, draft, busy, onRescore }: { score: ContentScore | null; draft: boolean; busy: boolean; onRescore: () => void }) {
  return (
    <section className="border border-outline-variant/40 bg-surface-lowest p-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs font-mono uppercase text-accent">Why this priority?</p>
          <p className="text-sm text-muted mt-1">{score?.reason || "No score explanation yet. Rescore this draft to generate one."}</p>
        </div>
        {draft && (
          <button type="button" onClick={onRescore} disabled={busy} className="px-3 py-2 border border-outline text-xs font-mono uppercase hover:bg-surface-hover disabled:opacity-40">
            {busy ? "Scoring..." : "Rescore"}
          </button>
        )}
      </div>
      {score?.factors && (
        <div className="mt-3 grid grid-cols-2 md:grid-cols-3 gap-2">
          {Object.entries(score.factors).map(([key, value]) => (
            <div key={key} className="border border-outline-variant/30 bg-surface px-2 py-1.5">
              <p className="text-[10px] font-mono uppercase text-dim">{titleCase(key)}</p>
              <p className="text-sm font-mono text-foreground">{value}/100</p>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

// ── Generate toolbar ────────────────────────────────────────────────────────

function GenerateToolbar({ onGenerated }: { onGenerated: () => void }) {
  const api = getApiBase();
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  async function generate(type: "hr-tracker" | "pitching-index" | "best-batters" | "best-pitchers" | "games-of-day" | "probables-board") {
    setBusy(type);
    setMsg(null);
    try {
      const res = await secureFetch(`${api}/cards/${type}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
        signal: AbortSignal.timeout(180_000),
      });
      const raw = await res.text();
      let data: { detail?: unknown; tweet_text?: string } = {};
      if (raw) {
        try {
          data = JSON.parse(raw) as typeof data;
        } catch {
          data = { detail: raw.slice(0, 400) };
        }
      }
      if (!res.ok) {
        const d = data.detail;
        const msg =
          typeof d === "string"
            ? d
            : Array.isArray(d)
              ? d.map((x) => (typeof x === "object" && x && "msg" in x ? String((x as { msg: string }).msg) : String(x))).join(" ")
              : "Request failed.";
        setMsg({ ok: false, text: msg });
      } else {
        setMsg({ ok: true, text: `Added to queue — ${data.tweet_text?.slice(0, 60)}…` });
        onGenerated();
      }
    } catch (e) {
      const hint =
        e instanceof DOMException && e.name === "TimeoutError"
          ? "Timed out after 3m (probables board downloads many headshots). Retry or run from CLI."
          : "";
      setMsg({ ok: false, text: [String(e), hint].filter(Boolean).join(" ") });
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="border-b border-outline-variant/30 bg-surface px-4 py-2 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
      <div className="flex flex-wrap items-center gap-4">
        <span className="text-xs font-mono text-outline uppercase tracking-tighter px-2 border-r border-outline-variant/30 shrink-0">
          QUICK_GEN
        </span>
        <div className="flex flex-wrap gap-2">
        <button
          type="button"
          onClick={() => void generate("games-of-day")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "games-of-day" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-accent">●</span>
          )}
          Games of Day
        </button>
        <button
          type="button"
          onClick={() => void generate("probables-board")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "probables-board" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-accent">◆</span>
          )}
          Probables board
        </button>
        <button
          type="button"
          onClick={() => void generate("hr-tracker")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "hr-tracker" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-danger">▶</span>
          )}
          HR Tracker
        </button>
        <button
          type="button"
          onClick={() => void generate("pitching-index")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "pitching-index" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-info">▣</span>
          )}
          Pitching Index
        </button>
        <button
          type="button"
          onClick={() => void generate("best-batters")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "best-batters" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-success">▲</span>
          )}
          Best Batters
        </button>
        <button
          type="button"
          onClick={() => void generate("best-pitchers")}
          disabled={busy !== null}
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 hover:border-accent transition-colors text-sm font-mono uppercase text-foreground disabled:opacity-40"
        >
          {busy === "best-pitchers" ? (
            <span className="h-3 w-3 rounded-full border-2 border-accent/30 border-t-accent animate-spin" />
          ) : (
            <span className="text-info">◆</span>
          )}
          Best Pitchers
        </button>
        <a
          href="/cards"
          className="inline-flex items-center gap-2 px-3 py-1.5 bg-surface-header border border-outline-variant/50 text-sm font-mono uppercase text-muted hover:text-foreground hover:border-accent transition-colors"
        >
          Player cards
        </a>
        </div>
      </div>
      <div className="text-xs font-mono text-slate-500 flex flex-wrap items-center gap-x-4 gap-y-1 shrink-0">
        <span>
          SYSTEM_STATUS: <span className="text-success">OPTIMAL</span>
        </span>
        <span>LATENCY: —</span>
      </div>
      {msg && (
        <p className={`w-full text-xs font-mono mt-2 sm:mt-0 ${msg.ok ? "text-success" : "text-danger"}`}>
          {msg.text}
        </p>
      )}
    </div>
  );
}

// ── List item ───────────────────────────────────────────────────────────────

function ItemRow({
  item,
  selected,
  onSelect,
}: {
  item: QueueItem;
  selected: boolean;
  onSelect: () => void;
}) {
  const typeLabel = CONTENT_TYPE_LABEL[item.content_type] ?? item.content_type.replace("_", " ");
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full text-left px-4 py-4 border-b border-outline-variant/20 hover:bg-surface-header transition-colors relative ${
        selected ? "bg-surface-hover/40 border-l-2 border-l-accent pl-[14px]" : "border-l-2 border-l-transparent"
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="text-sm font-headline font-semibold text-foreground truncate leading-tight">
            {item.player_name || item.title || "Untitled"}
          </p>
          <p className="text-xs text-dim mt-0.5">
            {typeLabel} · {item.game_date || "—"}
          </p>
        </div>
        <span
          className={`text-xs px-2 py-0.5 border whitespace-nowrap shrink-0 font-mono uppercase font-medium ${
            STATUS_COLORS[item.status] ?? "bg-surface text-muted border-border"
          }`}
        >
          {item.status}
        </span>
      </div>
    </button>
  );
}

// ── Detail panel ────────────────────────────────────────────────────────────

export default function QueueClient() {
  const api = getApiBase();
  const [activeTab, setActiveTab] = useState<StatusTab>("draft");
  const [sortVal, setSortVal] = useState<string>("created_at:desc");
  const [pillarFilter, setPillarFilter] = useState("");
  const [kpiFilter, setKpiFilter] = useState("");
  const [items, setItems] = useState<QueueItem[]>([]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [tweetText, setTweetText] = useState("");
  const [loading, setLoading] = useState(false);
  const [redraftLoading, setRedraftLoading] = useState(false);
  const [actionStatus, setActionStatus] = useState<{ type: "success" | "error"; msg: string } | null>(null);
  const [tweetMaxChars, setTweetMaxChars] = useState(10_000);
  const [apiUnreachable, setApiUnreachable] = useState<string | null>(null);
  const [quickText, setQuickText] = useState("");
  const [quickBusy, setQuickBusy] = useState(false);
  const [quickMsg, setQuickMsg] = useState<string | null>(null);
  const [streaks, setStreaks] = useState<StreakStats | null>(null);
  const [showPrompts, setShowPrompts] = useState(false);
  const [performance, setPerformance] = useState<PerformanceMetrics>(EMPTY_PERFORMANCE);
  const [performanceMsg, setPerformanceMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [performanceBusy, setPerformanceBusy] = useState(false);
  const [taxonomy, setTaxonomy] = useState<Taxonomy>(DEFAULT_TAXONOMY);
  const [scoreBusy, setScoreBusy] = useState(false);

  const selectedItem = items.find((i) => i.id === selectedId) ?? null;
  const selectedMeta = useMemo(() => parseMeta(selectedItem?.meta_json ?? null), [selectedItem?.meta_json]);
  const selectedScore = useMemo(() => {
    const raw = selectedMeta?.content_score;
    return raw && typeof raw === "object" && !Array.isArray(raw) ? raw as ContentScore : null;
  }, [selectedMeta]);

  const fetchSummary = useCallback(async () => {
    try {
      const res = await fetch(`${api}/queue/summary`);
      if (!res.ok) return;
      const data = await res.json();
      setCounts(data.by_status || {});
      setApiUnreachable(null);
    } catch {
      setApiUnreachable(apiDownMessage(api));
    }
  }, [api]);

  const fetchStreaks = useCallback(async () => {
    try {
      const res = await fetch("/api/queue/streaks", { cache: "no-store" });
      if (!res.ok) return;
      setStreaks((await res.json()) as StreakStats);
    } catch {
      /* optional dashboard metric */
    }
  }, []);

  const fetchItems = useCallback(async (tab: StatusTab, sort: string, keepSelected = false) => {
    const [col, ord] = sort.split(":");
    const params = new URLSearchParams({ limit: "50", sort_by: col, order: ord });
    if (tab !== "all") params.set("status", tab);
    if (pillarFilter) params.set("content_pillar", pillarFilter);
    if (kpiFilter) params.set("intended_kpi", kpiFilter);
    try {
      const res = await fetch(`${api}/queue?${params}`);
      if (!res.ok) return;
      const data = await res.json();
      setItems(data.items || []);
      if (!keepSelected) setSelectedId(null);
      setApiUnreachable(null);
    } catch {
      setApiUnreachable(apiDownMessage(api));
    }
  }, [api, pillarFilter, kpiFilter]);

  useEffect(() => {
    fetchSummary();
    fetchStreaks();
  }, [fetchSummary, fetchStreaks]);

  useEffect(() => {
    void (async () => {
      try {
        const res = await fetch(`${api}/queue/taxonomy`);
        if (!res.ok) return;
        setTaxonomy((await res.json()) as Taxonomy);
      } catch {
        setTaxonomy(DEFAULT_TAXONOMY);
      }
    })();
  }, [api]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const res = await fetch(`${api}/system/paths`);
        if (!res.ok || cancelled) return;
        const data = (await res.json()) as { tweet_max_chars?: number };
        if (typeof data.tweet_max_chars === "number" && data.tweet_max_chars > 0) {
          setTweetMaxChars(data.tweet_max_chars);
        }
      } catch {
        /* keep default */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [api]);
  useEffect(() => {
    fetchItems(activeTab, sortVal);
  }, [activeTab, sortVal, pillarFilter, kpiFilter, fetchItems]);
  useEffect(() => {
    if (selectedItem) setTweetText(selectedItem.tweet_text || "");
  }, [selectedItem]);

  useEffect(() => {
    setPerformanceMsg(null);
    if (!selectedItem || selectedItem.status !== "posted") {
      setPerformance(EMPTY_PERFORMANCE);
      return;
    }
    setPerformance({
      ...EMPTY_PERFORMANCE,
      x_post_id: selectedItem.twitter_post_id || "",
    });
    void (async () => {
      try {
        const res = await fetch(`${api}/analytics/performance/${selectedItem.id}`);
        if (res.status === 404) return;
        if (!res.ok) return;
        const data = (await res.json()) as PerformanceMetrics;
        setPerformance({
          ...EMPTY_PERFORMANCE,
          ...data,
          x_post_id: data.x_post_id || selectedItem.twitter_post_id || "",
          notes: data.notes || "",
        });
      } catch {
        /* performance entry is optional */
      }
    })();
  }, [api, selectedItem]);

  async function saveTweetText() {
    if (!selectedId) return;
    await secureFetch(`/api/queue/${selectedId}/tweet-text`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tweet_text: tweetText }),
    });
  }

  async function handleRedraft(provider: "claude" | "grok") {
    if (!selectedId) return;
    setRedraftLoading(true);
    setActionStatus(null);
    try {
      const res = await secureFetch(`${api}/queue/${selectedId}/redraft?provider=${provider}`, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        setActionStatus({ type: "error", msg: typeof data.detail === "string" ? data.detail : "Redraft failed." });
        return;
      }
      setTweetText(data.tweet_text || "");
      setActionStatus({ type: "success", msg: `${data.model} rewrote the draft — review and save.` });
    } catch (e) {
      setActionStatus({ type: "error", msg: String(e) });
    } finally {
      setRedraftLoading(false);
    }
  }

  async function handleApprove() {
    if (!selectedId) return;
    setLoading(true);
    setActionStatus(null);
    try {
      await saveTweetText();
      const needsConfirm = Boolean(
        selectedItem?.image_path || selectedMeta?.ai_assisted === true || selectedMeta?.creation_mode === "ai_assisted"
      );
      if (needsConfirm && !window.confirm("Post this reviewed draft to X?")) return;
      const res = await secureFetch(`/api/queue/${selectedId}/approve`, { method: "POST" });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: "success", msg: `Posted!${data.tweet_url ? " " + data.tweet_url : ""}` });
        await fetchItems(activeTab, sortVal);
        await fetchSummary();
        await fetchStreaks();
        setSelectedId(null);
      } else {
        setActionStatus({ type: "error", msg: data.error || "Post failed." });
      }
    } finally {
      setLoading(false);
    }
  }

  async function handleReject() {
    if (!selectedId) return;
    setLoading(true);
    setActionStatus(null);
    try {
      const res = await secureFetch(`/api/queue/${selectedId}/reject`, { method: "POST" });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: "success", msg: "Rejected." });
        await fetchItems(activeTab, sortVal);
        await fetchSummary();
        setSelectedId(null);
      } else {
        setActionStatus({ type: "error", msg: data.error || "Reject failed." });
      }
    } finally {
      setLoading(false);
    }
  }

  async function handleDeleteDraft() {
    if (!selectedId || selectedItem?.status !== "draft") return;
    const label = selectedItem.player_name || selectedItem.title || `queue #${selectedId}`;
    if (!window.confirm(`Delete this draft permanently?\n\n${label}`)) return;
    setLoading(true);
    setActionStatus(null);
    try {
      const res = await secureFetch(`${api}/queue/${selectedId}`, { method: "DELETE" });
      const data = await res.json().catch(() => ({}));
      if (res.ok) {
        setActionStatus({ type: "success", msg: "Draft deleted." });
        await fetchItems(activeTab, sortVal);
        await fetchSummary();
        setSelectedId(null);
      } else {
        const detail = data.detail || data.error;
        setActionStatus({ type: "error", msg: typeof detail === "string" ? detail : "Delete failed." });
      }
    } catch (e) {
      setActionStatus({ type: "error", msg: String(e) });
    } finally {
      setLoading(false);
    }
  }

  function handleGenerated() {
    fetchItems(activeTab, sortVal);
    fetchSummary();
  }

  async function handleQuickPost() {
    const text = quickText.trim();
    if (!text) return;
    setQuickBusy(true);
    setQuickMsg(null);
    try {
      const res = await secureFetch("/api/queue/quick-post", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tweet_text: text }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setQuickMsg(typeof data.error === "string" ? data.error : "Could not save draft.");
        return;
      }
      setQuickText("");
      setQuickMsg("Saved as a manual draft.");
      await fetchItems(activeTab, sortVal);
      await fetchSummary();
      if (typeof data.id === "number") setSelectedId(data.id);
    } finally {
      setQuickBusy(false);
    }
  }

  async function saveMetadata(patch: Record<string, string | number>) {
    if (!selectedId) return;
    setActionStatus(null);
    try {
      const res = await secureFetch(`${api}/queue/${selectedId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setActionStatus({ type: "error", msg: typeof data.detail === "string" ? data.detail : "Metadata save failed." });
        return;
      }
      setItems((rows) => rows.map((row) => row.id === selectedId ? data as QueueItem : row));
      setActionStatus({ type: "success", msg: "Metadata saved." });
    } catch (e) {
      setActionStatus({ type: "error", msg: String(e) });
    }
  }

  async function handleRescore() {
    if (!selectedId) return;
    setScoreBusy(true);
    setActionStatus(null);
    try {
      const res = await secureFetch(`${api}/queue/${selectedId}/score`, { method: "POST" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setActionStatus({ type: "error", msg: typeof data.detail === "string" ? data.detail : "Scoring failed." });
        return;
      }
      if (data.item) {
        setItems((rows) => rows.map((row) => row.id === selectedId ? data.item as QueueItem : row));
      } else {
        await fetchItems(activeTab, sortVal, true);
      }
      setActionStatus({ type: "success", msg: "Priority score updated." });
    } catch (e) {
      setActionStatus({ type: "error", msg: String(e) });
    } finally {
      setScoreBusy(false);
    }
  }

  function updatePerformanceField<K extends keyof PerformanceMetrics>(field: K, value: PerformanceMetrics[K]) {
    setPerformance((p) => ({ ...p, [field]: value }));
  }

  async function savePerformance() {
    if (!selectedItem) return;
    setPerformanceBusy(true);
    setPerformanceMsg(null);
    try {
      const res = await fetch(`${api}/analytics/performance/${selectedItem.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...performance,
          x_post_id: performance.x_post_id || selectedItem.twitter_post_id || "",
          posted_at: selectedItem.posted_at,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setPerformanceMsg({ ok: false, text: typeof data.detail === "string" ? data.detail : "Could not save metrics." });
        return;
      }
      setPerformance({ ...EMPTY_PERFORMANCE, ...(data as PerformanceMetrics), notes: (data as PerformanceMetrics).notes || "" });
      setPerformanceMsg({ ok: true, text: "Performance metrics saved." });
    } catch (e) {
      setPerformanceMsg({ ok: false, text: String(e) });
    } finally {
      setPerformanceBusy(false);
    }
  }

  return (
    <div className="flex flex-col flex-1 min-h-0 min-h-[calc(100dvh-12rem)] lg:min-h-[calc(100dvh-10rem)]">
      <section className="border-b border-outline-variant/30 bg-surface-container px-4 lg:px-6 py-4">
        <div className="max-w-5xl mx-auto grid gap-3 lg:grid-cols-[1fr_auto] lg:items-start">
          <div>
            <div className="flex items-center justify-between gap-3 mb-2">
              <div>
                <p className="text-xs font-mono uppercase tracking-widest text-accent">Write Now</p>
                <h2 className="text-lg font-headline font-bold text-foreground">Manual-first post</h2>
              </div>
              {streaks && (
                <div className="grid grid-cols-3 gap-2 text-center shrink-0">
                  <div className="border border-outline-variant/40 bg-surface px-2 py-1">
                    <p className="text-[10px] font-mono text-dim uppercase">Today</p>
                    <p className="text-lg font-headline font-bold text-foreground">{streaks.posted_today}</p>
                  </div>
                  <div className="border border-outline-variant/40 bg-surface px-2 py-1">
                    <p className="text-[10px] font-mono text-dim uppercase">Streak</p>
                    <p className="text-lg font-headline font-bold text-accent">{streaks.current_streak}</p>
                  </div>
                  <div className="border border-outline-variant/40 bg-surface px-2 py-1">
                    <p className="text-[10px] font-mono text-dim uppercase">Manual</p>
                    <p className="text-lg font-headline font-bold text-success">{streaks.manual_ratio}%</p>
                  </div>
                </div>
              )}
            </div>
            <textarea
              className="w-full min-h-[132px] border border-outline-variant/50 focus:border-accent bg-surface-lowest text-foreground px-3 py-3 text-base leading-relaxed outline-none resize-y"
              value={quickText}
              onChange={(e) => setQuickText(e.target.value)}
              placeholder="What did you notice?"
            />
            <div className="flex flex-wrap items-center justify-between gap-2 mt-2">
              <button
                type="button"
                onClick={() => setShowPrompts((v) => !v)}
                className="text-xs font-mono uppercase text-muted hover:text-foreground"
              >
                Need a nudge?
              </button>
              <div className="flex items-center gap-3">
                <CharCounter text={quickText} max={tweetMaxChars} />
                <button
                  type="button"
                  onClick={handleQuickPost}
                  disabled={quickBusy || !quickText.trim() || quickText.length > tweetMaxChars}
                  className="px-4 py-2 bg-accent text-[#552000] font-headline font-bold uppercase tracking-widest text-xs disabled:opacity-40"
                >
                  {quickBusy ? "Saving..." : "Save draft"}
                </button>
              </div>
            </div>
            {showPrompts && (
              <div className="mt-3 flex flex-wrap gap-2">
                {["What surprised you?", "One sentence before checking stats.", "What would you tell another fan?", "What changed your mind?"].map((prompt) => (
                  <button
                    key={prompt}
                    type="button"
                    onClick={() => setQuickText((t) => (t ? `${t}\n\n${prompt} ` : `${prompt} `))}
                    className="px-2 py-1 border border-outline-variant/50 bg-surface text-xs text-muted hover:text-foreground"
                  >
                    {prompt}
                  </button>
                ))}
              </div>
            )}
            {quickMsg && <p className="mt-2 text-xs font-mono text-success">{quickMsg}</p>}
          </div>
          {streaks && (
            <div className="hidden lg:block border border-outline-variant/40 bg-surface p-3 min-w-44">
              <p className="text-xs font-mono uppercase text-dim">Week</p>
              <p className="text-2xl font-headline font-bold text-foreground">{streaks.weekly_total}</p>
              <p className="text-xs text-dim">Longest streak: {streaks.longest_streak}</p>
            </div>
          )}
        </div>
      </section>
      <GenerateToolbar onGenerated={handleGenerated} />

      {apiUnreachable ? (
        <div className="border-b border-danger-border bg-danger-bg px-4 py-2 text-sm text-danger font-mono">
          {apiUnreachable}
        </div>
      ) : null}

      <div className="flex flex-col lg:flex-row flex-1 min-h-0 overflow-hidden gap-0 lg:gap-6 px-4 lg:px-6 pb-0">
        <section className="w-full lg:w-1/3 flex flex-col bg-surface border border-outline-variant/20 shrink-0 max-h-[42vh] lg:max-h-none min-h-0">
          <div className="flex border-b border-outline-variant/30">
            {STATUS_TABS.map((tab) => (
              <button
                key={tab}
                type="button"
                onClick={() => setActiveTab(tab)}
                className={`flex-1 py-3 text-xs font-mono uppercase border-b-2 transition-colors ${
                  activeTab === tab
                    ? "border-accent text-accent bg-accent/5"
                    : "border-transparent text-slate-500 hover:text-foreground"
                }`}
              >
                {tab}
                {tab !== "all" && counts[tab] != null && counts[tab] > 0 && (
                  <span className="ml-0.5 opacity-80">{counts[tab]}</span>
                )}
              </button>
            ))}
          </div>

          <div className="px-3 py-2 border-b border-outline-variant/30 grid gap-2 bg-surface-lowest/50">
            <label className="grid grid-cols-[auto_1fr] items-center gap-2">
              <span className="text-xs text-dim uppercase tracking-wide shrink-0 font-mono">Sort</span>
              <select
                value={sortVal}
                onChange={(e) => setSortVal(e.target.value)}
                className="border border-outline-variant bg-surface-lowest text-xs text-foreground py-1 px-1.5 font-mono"
              >
                {SORT_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </label>
            <div className="grid grid-cols-2 gap-2">
              <label className="text-[10px] font-mono uppercase text-dim">
                Pillar
                <select value={pillarFilter} onChange={(e) => setPillarFilter(e.target.value)} className="mt-1 w-full border border-outline-variant bg-surface-lowest text-xs text-foreground py-1 px-1.5 font-mono">
                  <option value="">All</option>
                  {taxonomy.content_pillars.map((v) => <option key={v} value={v}>{titleCase(v)}</option>)}
                </select>
              </label>
              <label className="text-[10px] font-mono uppercase text-dim">
                Primary KPI
                <select value={kpiFilter} onChange={(e) => setKpiFilter(e.target.value)} className="mt-1 w-full border border-outline-variant bg-surface-lowest text-xs text-foreground py-1 px-1.5 font-mono">
                  <option value="">All</option>
                  {taxonomy.intended_kpis.map((v) => <option key={v} value={v}>{titleCase(v)}</option>)}
                </select>
              </label>
            </div>
          </div>

          {/* Items */}
          <div className="flex-1 overflow-y-auto min-h-0">
            {items.length === 0 ? (
              <p className="text-sm text-dim text-center mt-8 px-4">
                No {activeTab === "all" ? "" : activeTab} items.
              </p>
            ) : (
              items.map((item) => (
                <ItemRow
                  key={item.id}
                  item={item}
                  selected={selectedId === item.id}
                  onSelect={() => setSelectedId(item.id)}
                />
              ))
            )}
          </div>
        </section>

        <section className="flex-1 flex flex-col bg-surface-container border border-outline-variant/20 p-4 lg:p-6 overflow-y-auto min-h-[50vh] lg:min-h-0">
          {!selectedItem ? (
            <div className="flex flex-col items-center justify-center h-48 gap-2 text-dim font-mono text-sm">
              <span className="text-2xl" aria-hidden>
                ↑
              </span>
              <p>Select a draft to review and post</p>
            </div>
          ) : (
            <div className="max-w-3xl mx-auto w-full space-y-5">
              <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3">
                <div>
                  <h2 className="text-xl font-headline font-bold text-foreground">
                    Launch Detail: {selectedItem.player_name || selectedItem.title}
                  </h2>
                  <p className="text-xs font-mono text-slate-500 mt-1 uppercase">
                    Queue ID #{selectedItem.id} /{" "}
                    {CONTENT_TYPE_LABEL[selectedItem.content_type] ?? selectedItem.content_type.replace("_", " ")}
                    {selectedItem.game_date ? ` · ${selectedItem.game_date}` : ""}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2 items-start">
                  {selectedItem.status === "draft" && (
                    <details className="relative">
                      <summary className="cursor-pointer px-3 py-2 border border-outline text-sm font-mono uppercase hover:bg-surface-hover transition-colors">
                        Need a nudge?
                      </summary>
                      <div className="absolute right-0 z-10 mt-1 w-48 border border-outline-variant bg-surface p-2 space-y-2 shadow-xl">
                        <button
                          type="button"
                          onClick={() => handleRedraft("claude")}
                          disabled={redraftLoading}
                          className="w-full px-3 py-2 border border-outline text-xs font-mono uppercase hover:bg-surface-hover transition-colors disabled:opacity-40"
                        >
                          {redraftLoading ? "…" : "Claude redraft"}
                        </button>
                        <button
                          type="button"
                          onClick={() => handleRedraft("grok")}
                          disabled={redraftLoading}
                          className="w-full px-3 py-2 border border-outline text-xs font-mono uppercase hover:bg-surface-hover transition-colors disabled:opacity-40"
                        >
                          {redraftLoading ? "…" : "Grok redraft"}
                        </button>
                      </div>
                    </details>
                  )}
                  <span
                    className={`text-xs px-2 py-1 border font-mono uppercase font-bold shrink-0 ${
                      STATUS_COLORS[selectedItem.status] ?? "bg-surface text-muted border-border"
                    }`}
                  >
                    {selectedItem.status}
                  </span>
                </div>
              </div>

              {selectedItem.image_url && (
                <div className="overflow-hidden border border-outline-variant/50 bg-surface-lowest">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={selectedItem.image_url}
                    alt={selectedItem.title || "Card"}
                    className="w-full h-auto object-contain"
                  />
                </div>
              )}

              <MetadataEditor item={selectedItem} taxonomy={taxonomy} onSave={saveMetadata} />
              <ScorePanel score={selectedScore} draft={selectedItem.status === "draft"} busy={scoreBusy} onRescore={handleRescore} />

              {/* Meta (collapsed, only if present) */}
              {selectedMeta && Object.keys(selectedMeta).length > 0 && (
                <details className="border border-outline-variant bg-surface-lowest text-xs">
                  <summary className="cursor-pointer px-3 py-2 text-dim hover:text-muted font-mono uppercase tracking-wide">
                    Source metadata
                  </summary>
                  <pre className="px-3 pb-3 text-muted overflow-x-auto whitespace-pre-wrap max-h-32 overflow-y-auto font-mono text-xs">
                    {JSON.stringify(selectedMeta, null, 2)}
                  </pre>
                </details>
              )}

              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <label className="text-xs font-mono text-accent uppercase tracking-wide">Tweet_Manifesto</label>
                  <CharCounter text={tweetText} max={tweetMaxChars} />
                </div>
                <textarea
                  className="w-full border-0 border-b-2 border-outline-variant focus:border-accent px-3 py-3 text-sm bg-surface-lowest text-foreground focus:outline-none focus:ring-0 resize-none leading-relaxed min-h-[140px]"
                  rows={6}
                  value={tweetText}
                  onChange={(e) => setTweetText(e.target.value)}
                  onBlur={saveTweetText}
                  disabled={selectedItem.status !== "draft"}
                />
              </div>

              {selectedItem.status === "draft" && (
                <div className="grid grid-cols-3 gap-3">
                  <button
                    type="button"
                    onClick={() => void saveTweetText()}
                    className="py-3 bg-surface-header border border-outline hover:bg-surface-hover text-sm font-mono uppercase text-foreground transition-all"
                  >
                    Save progress
                  </button>
                  <button
                    type="button"
                    onClick={handleReject}
                    disabled={loading}
                    className="py-3 bg-danger-bg/30 border border-danger hover:bg-danger-bg/50 text-sm font-mono uppercase text-danger disabled:opacity-40 transition-all"
                  >
                    Reject
                  </button>
                  <button
                    type="button"
                    onClick={handleDeleteDraft}
                    disabled={loading}
                    className="py-3 bg-surface-lowest border border-danger/60 hover:bg-danger-bg/40 text-sm font-mono uppercase text-danger disabled:opacity-40 transition-all"
                  >
                    Delete draft
                  </button>
                  <button
                    type="button"
                    onClick={handleApprove}
                    disabled={loading || !tweetText.trim() || tweetText.length > tweetMaxChars}
                    className="col-span-3 py-4 bg-accent text-[#552000] font-headline font-bold uppercase tracking-widest text-xs hover:brightness-110 active:scale-[0.99] transition-all flex items-center justify-center gap-3 disabled:opacity-40"
                  >
                    <span>APPROVE & POST TO X</span>
                    <span className="material-symbols-outlined text-sm" aria-hidden>
                      rocket_launch
                    </span>
                  </button>
                </div>
              )}

              {selectedItem.status === "posted" && selectedItem.twitter_post_id && (
                <div className="border border-success-border bg-success-bg px-4 py-3 text-sm text-success font-mono">
                  Posted · {selectedItem.twitter_post_id}
                </div>
              )}
              {selectedItem.status === "posted" && (
                <section className="border border-outline-variant/50 bg-surface-lowest p-4">
                  <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 mb-3">
                    <div>
                      <p className="text-xs font-mono uppercase tracking-wide text-accent">Performance</p>
                      <h3 className="text-sm font-headline font-bold text-foreground">Manual X metrics</h3>
                    </div>
                    {typeof performance.engagement_rate === "number" && (
                      <div className="text-xs font-mono text-dim">
                        ENG {(performance.engagement_rate * 100).toFixed(2)}% · BKM {(Number(performance.bookmark_rate || 0) * 100).toFixed(2)}%
                      </div>
                    )}
                  </div>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                    {([
                      ["impressions", "Impressions"],
                      ["likes", "Likes"],
                      ["replies", "Replies"],
                      ["reposts", "Reposts"],
                      ["quote_tweets", "Quotes"],
                      ["bookmarks", "Bookmarks"],
                      ["profile_visits", "Profile visits"],
                      ["follows", "Follows"],
                    ] as const).map(([field, label]) => (
                      <label key={field} className="text-[10px] font-mono uppercase text-dim">
                        {label}
                        <input
                          type="number"
                          min={0}
                          value={performance[field] ?? 0}
                          onChange={(e) => updatePerformanceField(field, Number(e.target.value || 0))}
                          className="mt-1 w-full border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-sm font-mono"
                        />
                      </label>
                    ))}
                  </div>
                  <label className="block mt-3 text-[10px] font-mono uppercase text-dim">
                    Notes
                    <textarea
                      value={performance.notes || ""}
                      onChange={(e) => updatePerformanceField("notes", e.target.value)}
                      className="mt-1 w-full border border-outline-variant bg-surface text-foreground px-2 py-2 text-sm min-h-16"
                    />
                  </label>
                  <div className="mt-3 flex flex-wrap items-center gap-3">
                    <button
                      type="button"
                      onClick={savePerformance}
                      disabled={performanceBusy}
                      className="px-3 py-2 bg-accent text-[#552000] font-headline font-bold uppercase tracking-widest text-xs disabled:opacity-40"
                    >
                      {performanceBusy ? "Saving..." : "Save metrics"}
                    </button>
                    {performanceMsg && (
                      <span className={`text-xs font-mono ${performanceMsg.ok ? "text-success" : "text-danger"}`}>
                        {performanceMsg.text}
                      </span>
                    )}
                  </div>
                </section>
              )}
              {selectedItem.status === "failed" && selectedItem.error_message && (
                <div className="border border-danger-border bg-danger-bg px-4 py-3 text-sm text-danger font-mono">
                  {selectedItem.error_message}
                </div>
              )}

              {actionStatus && (
                <div
                  className={`px-4 py-3 text-sm border font-mono ${
                    actionStatus.type === "success"
                      ? "bg-success-bg text-success border-success-border"
                      : "bg-danger-bg text-danger border-danger-border"
                  }`}
                >
                  {actionStatus.msg}
                </div>
              )}
            </div>
          )}
        </section>
      </div>

      <footer className="bg-black border-t border-outline-variant/30 p-3 flex flex-wrap items-center gap-4 shrink-0 mt-auto">
        <span className="text-accent text-xs font-mono shrink-0">CONSOLE_V2.4_READY &gt;</span>
        <div className="flex-1 overflow-hidden min-w-[8rem]">
          <p className="text-xs font-mono text-slate-500 animate-pulse whitespace-nowrap truncate">
            Queue ready · select a draft to edit tweet text · API {api ? "reachable" : "—"}
          </p>
        </div>
        <div className="flex gap-4 shrink-0 items-center">
          {counts["posted"] != null && (
            <div className="flex items-center gap-1.5 border border-success-border bg-success-bg px-2 py-0.5">
              <span className="text-xs font-mono text-success uppercase tracking-widest">Posted</span>
              <span className="text-sm font-mono font-bold text-success tabular-nums">{counts["posted"]}</span>
            </div>
          )}
          <div className="flex items-center gap-2">
            <span className="w-1.5 h-1.5 bg-success" aria-hidden />
            <span className="text-xs font-mono text-slate-400 uppercase">Live</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-1.5 h-1.5 bg-slate-700" aria-hidden />
            <span className="text-xs font-mono text-slate-400 uppercase">Hub</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
