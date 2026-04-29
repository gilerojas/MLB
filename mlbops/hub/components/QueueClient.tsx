"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { QueueItem } from "@/lib/db";
import { getApiBase } from "@/lib/api";

const STATUS_TABS = ["draft", "all", "approved", "posted", "rejected", "failed"] as const;
type StatusTab = (typeof STATUS_TABS)[number];

const SORT_OPTIONS = [
  { value: "created_at:desc", label: "Newest" },
  { value: "created_at:asc", label: "Oldest" },
  { value: "game_date:desc", label: "Game date ↓" },
  { value: "game_date:asc", label: "Game date ↑" },
] as const;

const STATUS_COLORS: Record<string, string> = {
  draft:    "bg-accent-bg-active text-accent border border-accent/35",
  approved: "bg-info-bg text-info border border-info-border",
  posted:   "bg-success-bg text-success border border-success-border",
  rejected: "bg-danger-bg text-danger border border-danger-border",
  failed:   "bg-warning-bg text-warning border border-warning-border",
};

function apiDownMessage(apiBase: string): string {
  return `Cannot reach the MLB Ops API at ${apiBase}. Start FastAPI on port 8000 (e.g. ./start_hub.sh from the MLB repo root). If you only ran npm run dev, the API is not up. Open the hub at http://127.0.0.1:3000 (or your port) so the browser targets 127.0.0.1:8000.`;
}

const CONTENT_TYPE_LABEL: Record<string, string> = {
  batter_card:      "Batter card",
  pitcher_card:     "Pitcher card",
  hr_tracker:       "HR Tracker",
  games_of_day:     "Games of Day",
  probables_board:  "Probables board",
  insight_tile:     "Insight",
  live_event:       "Live event",
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

function parseMeta(raw: string | null): Record<string, unknown> | null {
  if (!raw) return null;
  try { return JSON.parse(raw) as Record<string, unknown>; } catch { return null; }
}

// ── Generate toolbar ────────────────────────────────────────────────────────

function GenerateToolbar({ onGenerated }: { onGenerated: () => void }) {
  const api = getApiBase();
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  async function generate(type: "hr-tracker" | "games-of-day" | "probables-board") {
    setBusy(type);
    setMsg(null);
    try {
      const res = await fetch(`${api}/cards/${type}`, {
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
  const [items, setItems] = useState<QueueItem[]>([]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [tweetText, setTweetText] = useState("");
  const [loading, setLoading] = useState(false);
  const [redraftLoading, setRedraftLoading] = useState(false);
  const [actionStatus, setActionStatus] = useState<{ type: "success" | "error"; msg: string } | null>(null);
  const [tweetMaxChars, setTweetMaxChars] = useState(10_000);
  const [apiUnreachable, setApiUnreachable] = useState<string | null>(null);

  const selectedItem = items.find((i) => i.id === selectedId) ?? null;
  const selectedMeta = useMemo(() => parseMeta(selectedItem?.meta_json ?? null), [selectedItem?.meta_json]);

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

  const fetchItems = useCallback(async (tab: StatusTab, sort: string) => {
    const [col, ord] = sort.split(":");
    const params = new URLSearchParams({ limit: "50", sort_by: col, order: ord });
    if (tab !== "all") params.set("status", tab);
    try {
      const res = await fetch(`${api}/queue?${params}`);
      if (!res.ok) return;
      const data = await res.json();
      setItems(data.items || []);
      setApiUnreachable(null);
    } catch {
      setApiUnreachable(apiDownMessage(api));
    }
  }, [api]);

  useEffect(() => { fetchSummary(); }, [fetchSummary]);

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
    setSelectedId(null);
  }, [activeTab, sortVal, fetchItems]);
  useEffect(() => {
    if (selectedItem) setTweetText(selectedItem.tweet_text || "");
  }, [selectedItem]);

  async function saveTweetText() {
    if (!selectedId) return;
    await fetch(`/api/queue/${selectedId}/tweet-text`, {
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
      const res = await fetch(`${api}/queue/${selectedId}/redraft?provider=${provider}`, { method: "POST" });
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
      const res = await fetch(`/api/queue/${selectedId}/approve`, { method: "POST" });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: "success", msg: `Posted!${data.tweet_url ? " " + data.tweet_url : ""}` });
        await fetchItems(activeTab, sortVal);
        await fetchSummary();
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
      const res = await fetch(`/api/queue/${selectedId}/reject`, { method: "POST" });
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

  function handleGenerated() {
    fetchItems(activeTab, sortVal);
    fetchSummary();
  }

  return (
    <div className="flex flex-col flex-1 min-h-0 min-h-[calc(100dvh-12rem)] lg:min-h-[calc(100dvh-10rem)]">
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

          <div className="px-3 py-2 border-b border-outline-variant/30 flex items-center gap-2 bg-surface-lowest/50">
            <span className="text-xs text-dim uppercase tracking-wide shrink-0 font-mono">Sort</span>
            <select
              value={sortVal}
              onChange={(e) => setSortVal(e.target.value)}
              className="flex-1 border border-outline-variant bg-surface-lowest text-xs text-foreground py-1 px-1.5 font-mono"
            >
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
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
                    <>
                      <button
                        type="button"
                        onClick={() => handleRedraft("claude")}
                        disabled={redraftLoading}
                        className="px-4 py-2 border border-outline text-sm font-mono uppercase hover:bg-surface-hover transition-colors disabled:opacity-40"
                      >
                        {redraftLoading ? "…" : "Redraft (Claude)"}
                      </button>
                      <button
                        type="button"
                        onClick={() => handleRedraft("grok")}
                        disabled={redraftLoading}
                        className="px-4 py-2 border border-outline text-sm font-mono uppercase hover:bg-surface-hover transition-colors disabled:opacity-40"
                      >
                        {redraftLoading ? "…" : "Redraft (Grok)"}
                      </button>
                    </>
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
                <div className="grid grid-cols-2 gap-3">
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
                    onClick={handleApprove}
                    disabled={loading || !tweetText.trim() || tweetText.length > tweetMaxChars}
                    className="col-span-2 py-4 bg-accent text-[#552000] font-headline font-bold uppercase tracking-widest text-xs hover:brightness-110 active:scale-[0.99] transition-all flex items-center justify-center gap-3 disabled:opacity-40"
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
