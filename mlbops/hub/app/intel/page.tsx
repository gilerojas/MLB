import type { ReactNode } from "react";
import Link from "next/link";
import { IntelStandoutsPanel } from "@/components/IntelStandoutsPanel";
import { MorningIntelRunPanel } from "@/components/MorningIntelRunPanel";
import { serverApiFetch } from "@/lib/server-api";

export const dynamic = "force-dynamic";

interface SnapshotMeta {
  anchor: string;
  path: string;
  data?: Record<string, unknown>;
  error?: string;
}

interface IntelListResponse {
  snapshots: SnapshotMeta[];
  count: number;
}

interface SystemPathsPayload {
  intel_snapshots_dir?: string;
  intel_run_allowed?: boolean;
}

// ── helpers ──────────────────────────────────────────────────────────────────

function formatAnchorHeading(anchor: string): string {
  try {
    const d = new Date(`${anchor}T12:00:00`);
    if (Number.isNaN(d.getTime())) return anchor;
    return d.toLocaleDateString("en-US", {
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  } catch {
    return anchor;
  }
}

/** Split bundled newline-separated blobs into one row per item. */
function expandLines(items: string[]): string[] {
  const out: string[] = [];
  for (const t of items) {
    if (!t || !String(t).trim()) continue;
    const parts = String(t).split(/\n+/).map((s) => s.trim()).filter(Boolean);
    out.push(...(parts.length ? parts : [String(t).trim()]));
  }
  return out;
}

// ── team logo lookup ──────────────────────────────────────────────────────────

const TEAM_NAME_TO_ID: Record<string, number> = {
  "yankees": 147, "new york yankees": 147,
  "mets": 121, "new york mets": 121,
  "red sox": 111, "boston red sox": 111,
  "blue jays": 141, "toronto blue jays": 141,
  "orioles": 110, "baltimore orioles": 110,
  "rays": 139, "tampa bay rays": 139,
  "white sox": 145, "chicago white sox": 145,
  "guardians": 114, "cleveland guardians": 114,
  "tigers": 116, "detroit tigers": 116,
  "royals": 118, "kansas city royals": 118,
  "twins": 142, "minnesota twins": 142,
  "astros": 117, "houston astros": 117,
  "angels": 108, "los angeles angels": 108,
  "athletics": 133, "oakland athletics": 133, "sacramento athletics": 133,
  "mariners": 136, "seattle mariners": 136,
  "rangers": 140, "texas rangers": 140,
  "braves": 144, "atlanta braves": 144,
  "marlins": 146, "miami marlins": 146,
  "phillies": 143, "philadelphia phillies": 143,
  "nationals": 120, "washington nationals": 120,
  "cubs": 112, "chicago cubs": 112,
  "reds": 113, "cincinnati reds": 113,
  "brewers": 158, "milwaukee brewers": 158,
  "pirates": 134, "pittsburgh pirates": 134,
  "cardinals": 138, "st. louis cardinals": 138, "st louis cardinals": 138,
  "diamondbacks": 109, "arizona diamondbacks": 109,
  "rockies": 115, "colorado rockies": 115,
  "dodgers": 119, "los angeles dodgers": 119,
  "padres": 135, "san diego padres": 135,
  "giants": 137, "san francisco giants": 137,
};

function findTeamIdInText(text: string): number | null {
  const lower = text.toLowerCase();
  // Try longest matches first to avoid "giants" matching inside "san francisco giants"
  const sorted = Object.keys(TEAM_NAME_TO_ID).sort((a, b) => b.length - a.length);
  for (const name of sorted) {
    if (lower.includes(name)) return TEAM_NAME_TO_ID[name];
  }
  return null;
}

// ── transaction parsing ───────────────────────────────────────────────────────

interface ParsedTx {
  date: string;   // "03/31"
  name: string;
  type: string;
  desc: string;
  teamId: number | null;
}

/** Parse "[MM/DD] Name: Type — Description" or fall back gracefully. */
function parseTransaction(line: string): ParsedTx {
  const m = line.match(/^\[(\d{2}\/\d{2})\]\s+(.+?):\s+(.+?)\s+—\s+(.+)$/);
  if (m) {
    const teamId = findTeamIdInText(m[4]) ?? findTeamIdInText(m[3]);
    return { date: m[1], name: m[2], type: m[3], desc: m[4], teamId };
  }
  // Fallback: no date prefix
  const m2 = line.match(/^(.+?):\s+(.+?)\s+—\s+(.+)$/);
  if (m2) {
    const teamId = findTeamIdInText(m2[3]) ?? findTeamIdInText(m2[2]);
    return { date: "", name: m2[1], type: m2[2], desc: m2[3], teamId };
  }
  return { date: "", name: line, type: "", desc: "", teamId: findTeamIdInText(line) };
}

/** Prefer `transactions_detail` from morning intel (MLB team_id); else parse legacy string lines. */
function parsedTransactionsFromSnapshot(d: Record<string, unknown>): ParsedTx[] {
  const lines = ((d.transactions as string[]) || []).map((x) => String(x));
  const detail = d.transactions_detail;
  if (Array.isArray(detail) && detail.length > 0) {
    return detail.map((raw) => {
      const row = raw && typeof raw === "object" ? (raw as Record<string, unknown>) : {};
      const line = String(row.line ?? "");
      const rawTid = row.team_id;
      let teamId: number | null = null;
      if (typeof rawTid === "number" && Number.isFinite(rawTid) && rawTid > 0) {
        teamId = rawTid;
      } else if (typeof rawTid === "string" && /^\d+$/.test(rawTid.trim())) {
        teamId = parseInt(rawTid.trim(), 10);
      }
      const base = parseTransaction(line);
      return { ...base, teamId: teamId ?? base.teamId };
    });
  }
  return lines.map(parseTransaction);
}

function txBadgeClass(type: string): string {
  const t = type.toLowerCase();
  if (t.includes("designated") || t.includes("release") || t.includes("outrighted"))
    return "text-red-300 bg-red-950/40 border-red-800/40";
  if (t.includes("trade"))
    return "text-orange-300 bg-orange-950/40 border-orange-800/40";
  if (t.includes("selected") || t.includes("recalled") || t.includes("activated"))
    return "text-emerald-300 bg-emerald-950/40 border-emerald-800/40";
  if (t.includes("optioned") || t.includes("assigned"))
    return "text-sky-300 bg-sky-950/40 border-sky-800/40";
  if (t.includes("signed"))
    return "text-violet-300 bg-violet-950/40 border-violet-800/40";
  return "text-muted bg-surface-hover border-border";
}

// ── sub-components ────────────────────────────────────────────────────────────

/** Snapshot JSON must be arrays; ignore null / wrong types so we never render an empty column by mistake. */
function anomalyRows(raw: unknown): Record<string, unknown>[] {
  if (!Array.isArray(raw)) return [];
  return raw.filter((x): x is Record<string, unknown> => x != null && typeof x === "object");
}

function SectionLabel({ children, subtle }: { children: ReactNode; subtle?: boolean }) {
  return (
    <p
      className={`font-headline font-bold uppercase tracking-widest text-muted border-b border-accent pb-1 mb-3 inline-block ${
        subtle ? "text-xs" : "text-xs"
      }`}
    >
      {children}
    </p>
  );
}

/** Statcast-style pitch type abbreviations → short readable labels */
const PITCH_USAGE_LABEL: Record<string, string> = {
  FF: "4-seam usage",
  FA: "Fastball usage",
  SI: "Sinker usage",
  FC: "Cutter usage",
  CH: "Changeup usage",
  FS: "Splitter usage",
  SL: "Slider usage",
  ST: "Sweeper usage",
  SV: "Slurve usage",
  KC: "Knuckle curve usage",
  CU: "Curve usage",
  CS: "Slow curve usage",
  EP: "Eephus usage",
  KN: "Knuckleball usage",
  SC: "Screwball usage",
  FO: "Forkball usage",
  PO: "Pitchout",
  UN: "Unknown pitch usage",
};

function metricLabel(metric: string): string {
  const mix = /^mix_([A-Za-z]+)_pct$/.exec(metric);
  if (mix) {
    const abbr = mix[1].toUpperCase();
    return PITCH_USAGE_LABEL[abbr] || `${abbr} mix`;
  }
  switch (metric) {
    case "avg_velo_mph":
      return "Avg release speed";
    case "whiff_pct":
      return "Whiff rate";
    case "chase_pct":
      return "Chase rate";
    case "xwoba_on_BIP":
      return "xwOBA on balls in play";
    case "avg_EV_mph":
      return "Avg exit velocity";
    case "barrel_pct":
      return "Barrel rate";
    default:
      return metric.replace(/_/g, " ");
  }
}

/** One plain-language line: what moved, how much, over what window */
function anomalyBlurb(row: Record<string, unknown>): string {
  const metric = String(row.metric ?? "");
  const label = metricLabel(metric);
  const windowLabel =
    typeof row.window_label === "string" && row.window_label.trim()
      ? row.window_label.trim()
      : null;
  const w = row.window_days != null ? Number(row.window_days) : null;
  const dRaw = row.delta;
  const n = typeof dRaw === "number" ? dRaw : parseFloat(String(dRaw));
  if (!Number.isFinite(n)) {
    return `${label} (see JSON)`;
  }
  const windowPhrase =
    windowLabel ??
    (w != null && Number.isFinite(w) ? `last ${w} days vs prior stretch` : "vs baseline");

  if (metric === "avg_velo_mph") {
    const dir = n > 0 ? "up" : "down";
    return `${label} ${dir} ${Math.abs(n).toFixed(1)} mph (${windowPhrase})`;
  }
  if (metric === "avg_EV_mph") {
    const dir = n > 0 ? "up" : "down";
    return `${label} ${dir} ${Math.abs(n).toFixed(1)} mph (${windowPhrase})`;
  }
  if (metric === "xwoba_on_BIP") {
    const dir = n > 0 ? "up" : "down";
    return `${label} ${dir} ${Math.abs(n).toFixed(3)} (${windowPhrase})`;
  }
  // Percentage-point style metrics (whiff, chase, barrel, pitch mix)
  const dir = n > 0 ? "up" : "down";
  return `${label} ${dir} ${Math.abs(n).toFixed(1)} pts (${windowPhrase})`;
}

function AnomalyList({ rows }: { rows: Record<string, unknown>[] }) {
  if (!rows?.length) {
    return <p className="text-xs text-dim py-0.5">None flagged.</p>;
  }
  return (
    <ul className="space-y-1.5 max-h-[220px] overflow-y-auto pr-1 overscroll-contain">
      {rows.map((a, i) => {
        const name = (a.player_name as string) || `Player ${a.player_id}`;
        const blurb = anomalyBlurb(a);
        return (
          <li
            key={i}
            className="flex flex-col gap-0.5 rounded-md border border-border/60 bg-surface-hover/40 px-2 py-1.5 sm:flex-row sm:items-start sm:gap-2"
          >
            <span className="text-xs font-semibold text-foreground shrink-0 sm:w-[9.5rem] sm:truncate">
              {name}
            </span>
            <span className="text-sm text-muted leading-snug min-w-0">{blurb}</span>
          </li>
        );
      })}
    </ul>
  );
}

function TransactionRow({ tx }: { tx: ParsedTx }) {
  const isError = tx.name.startsWith("(transactions API");
  if (isError) {
    return (
      <div className="py-3 border-b border-border/40 text-xs text-dim">{tx.name}</div>
    );
  }
  const tid = tx.teamId != null && tx.teamId > 0 ? tx.teamId : null;
  return (
    <div className="flex gap-2 py-3 border-b border-border/40 last:border-0 items-start">
      {tid ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={`https://www.mlbstatic.com/team-logos/${tid}.svg`}
          alt=""
          width={28}
          height={28}
          className="w-7 h-7 object-contain shrink-0 mt-0.5"
        />
      ) : (
        <div
          className="w-7 h-7 shrink-0 rounded bg-surface-hover border border-border/50 mt-0.5"
          aria-hidden
        />
      )}
      <div className="min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
          {tx.date && (
            <span className="font-mono text-xs font-semibold text-info/80">{tx.date}</span>
          )}
          {tx.type && (
            <span
              className={`inline-block text-xs font-medium px-1.5 py-0.5 rounded border leading-tight ${txBadgeClass(tx.type)}`}
            >
              {tx.type}
            </span>
          )}
        </div>
        <p className="text-sm font-semibold text-foreground leading-snug mt-0.5">{tx.name}</p>
        {tx.desc && (
          <p className="text-sm text-muted leading-relaxed mt-0.5">{tx.desc}</p>
        )}
      </div>
    </div>
  );
}

function LineList({ items, empty }: { items: string[]; empty: string }) {
  const lines = expandLines(items);
  if (!lines.length) return <p className="text-sm text-dim">{empty}</p>;
  return (
    <ul className="space-y-1.5">
      {lines.map((l, i) => (
        <li key={i} className="text-sm text-foreground-muted leading-relaxed">
          {l}
        </li>
      ))}
    </ul>
  );
}

// ── watchlist pulse (Statcast window + season vs last season) ────────────────

interface WatchlistCompare {
  role: "batter" | "pitcher";
  stat: string;
  this_value: string | null;
  last_value: string | null;
  this_n?: number | null;
  n_label?: string;
  this_volume?: string | null;
  last_volume?: string | null;
}

interface WatchlistPulseDetail {
  player_id: number;
  player_name: string;
  recent_lines: string[];
  compares: WatchlistCompare[];
  season: number;
  prev_season: number;
}

function coerceWatchlistPulseDetail(raw: unknown): WatchlistPulseDetail[] {
  if (!Array.isArray(raw)) return [];
  const out: WatchlistPulseDetail[] = [];
  for (const x of raw) {
    if (!x || typeof x !== "object") continue;
    const o = x as Record<string, unknown>;
    const name = o.player_name;
    if (typeof name !== "string") continue;
    const pid =
      typeof o.player_id === "number" ? o.player_id : parseInt(String(o.player_id), 10) || 0;
    const rl = o.recent_lines;
    const recent_lines = Array.isArray(rl)
      ? rl.filter((l): l is string => typeof l === "string")
      : [];
    const compRaw = o.compares;
    const compares: WatchlistCompare[] = [];
    if (Array.isArray(compRaw)) {
      for (const c of compRaw) {
        if (!c || typeof c !== "object") continue;
        const r = c as Record<string, unknown>;
        const role = r.role === "pitcher" ? "pitcher" : "batter";
        compares.push({
          role,
          stat: String(r.stat ?? ""),
          this_value: r.this_value == null || r.this_value === "" ? null : String(r.this_value),
          last_value: r.last_value == null || r.last_value === "" ? null : String(r.last_value),
          this_n: typeof r.this_n === "number" ? r.this_n : null,
          n_label: typeof r.n_label === "string" ? r.n_label : undefined,
          this_volume: r.this_volume == null ? null : String(r.this_volume),
          last_volume: r.last_volume == null ? null : String(r.last_volume),
        });
      }
    }
    out.push({
      player_id: pid,
      player_name: name,
      recent_lines,
      compares,
      season: typeof o.season === "number" ? o.season : Number(o.season) || 0,
      prev_season: typeof o.prev_season === "number" ? o.prev_season : Number(o.prev_season) || 0,
    });
  }
  return out;
}

function WatchlistPulseList({
  detail,
  legacyLines,
}: {
  detail: WatchlistPulseDetail[];
  legacyLines: string[];
}) {
  if (detail.length === 0) {
    return <LineList items={legacyLines} empty="—" />;
  }
  return (
    <ul className="space-y-2 max-h-[320px] overflow-y-auto pr-1 overscroll-contain">
      {detail.map((row) => (
        <li
          key={row.player_id}
          className="rounded-md border border-border/70 bg-surface-hover/35 px-3 py-2.5"
        >
          <p className="text-xs font-semibold text-foreground">{row.player_name}</p>
          <div className="mt-1.5 space-y-1">
            {row.recent_lines.map((ln, i) => (
              <p key={i} className="text-sm text-muted leading-snug">
                {ln}
              </p>
            ))}
          </div>
          {row.compares.length > 0 && (
            <div className="mt-2 pt-2 border-t border-border/50 space-y-2">
              <p className="text-xs uppercase tracking-wide text-dim">
                Season rates · {row.season} vs {row.prev_season}
              </p>
              {row.compares.map((c, i) => (
                <div
                  key={i}
                  className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm leading-tight"
                >
                  <span
                    className={`text-xs font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded border shrink-0 ${
                      c.role === "pitcher"
                        ? "border-info-border text-info bg-info-bg/35"
                        : "border-success-border text-success bg-success-bg/35"
                    }`}
                  >
                    {c.stat}
                  </span>
                  <span className="font-mono tabular-nums text-foreground">{c.this_value ?? "—"}</span>
                  <span className="text-dim">vs</span>
                  <span className="font-mono tabular-nums text-foreground-muted">
                    {c.last_value ?? "—"}
                  </span>
                  <span className="text-xs text-dim">prior</span>
                  {c.this_volume ? (
                    <span className="text-xs text-dim ml-auto sm:ml-0">{c.this_volume}</span>
                  ) : null}
                </div>
              ))}
            </div>
          )}
        </li>
      ))}
    </ul>
  );
}

// ── milestone watch (structured snapshot + legacy string fallback) ───────────

interface MilestoneDetail {
  player_id: number;
  player_name: string;
  stat: string;
  group: string;
  current: number;
  target: number;
  need: number;
  unit: string;
  team_id?: number | null;
}

function milestoneKindLabel(d: Pick<MilestoneDetail, "stat" | "group">): string {
  if (d.stat === "HR" && d.group === "hitting") return "Season home runs (next round mark)";
  if (d.stat === "2B" && d.group === "hitting") return "Season doubles";
  if (d.stat === "3B" && d.group === "hitting") return "Season triples";
  if (d.stat === "bK" && d.group === "hitting") return "Season batter strikeouts";
  if (d.stat === "H" && d.group === "hitting") return "Season hits (career-style totals)";
  if (d.stat === "K" && d.group === "pitching") return "Season pitcher strikeouts";
  if (d.stat === "SV" && d.group === "pitching") return "Season saves";
  if (d.stat === "CG" && d.group === "pitching") return "Season complete games";
  return `Season ${d.stat}`;
}

function parseMilestoneStrings(lines: string[]): MilestoneDetail[] {
  const re = /^(.+?): (\d+) (\S+) · (\d+) away from (\d+) (.+)$/;
  const out: MilestoneDetail[] = [];
  for (const line of lines) {
    const m = line.match(re);
    if (!m) continue;
    const cur = parseInt(m[2], 10);
    const need = parseInt(m[4], 10);
    const tgt = parseInt(m[5], 10);
    const stat = m[3];
    const pitchingStats = new Set(["K", "SV", "CG"]);
    out.push({
      player_name: m[1].trim(),
      player_id: 0,
      stat,
      group: pitchingStats.has(stat) ? "pitching" : "hitting",
      current: cur,
      target: tgt,
      need,
      unit: m[6].trim(),
      team_id: null,
    });
  }
  return out;
}

function coerceMilestoneDetail(raw: unknown): MilestoneDetail[] {
  if (!Array.isArray(raw)) return [];
  const out: MilestoneDetail[] = [];
  for (const x of raw) {
    if (!x || typeof x !== "object") continue;
    const o = x as Record<string, unknown>;
    const name = o.player_name;
    if (typeof name !== "string") continue;
    const cur = Number(o.current);
    const tgt = Number(o.target);
    const need = Number(o.need);
    if (!Number.isFinite(cur) || !Number.isFinite(tgt)) continue;
    out.push({
      player_id: typeof o.player_id === "number" ? o.player_id : parseInt(String(o.player_id), 10) || 0,
      player_name: name,
      stat: String(o.stat ?? ""),
      group: String(o.group ?? "hitting"),
      current: cur,
      target: tgt,
      need: Number.isFinite(need) ? need : tgt - cur,
      unit: String(o.unit ?? ""),
      team_id: o.team_id == null ? null : Number(o.team_id),
    });
  }
  return out;
}

function milestoneRowCount(detail: MilestoneDetail[], legacyLines: string[]): number {
  if (detail.length > 0) return detail.length;
  return parseMilestoneStrings(legacyLines).length;
}

function MilestoneWatchList({
  detail,
  legacyLines,
}: {
  detail: MilestoneDetail[];
  legacyLines: string[];
}) {
  const rows = detail.length > 0 ? detail : parseMilestoneStrings(legacyLines);
  if (!rows.length) return null;
  return (
    <ul className="space-y-1.5 max-h-[280px] overflow-y-auto pr-1 overscroll-contain">
      {rows.map((d, i) => (
        <li
          key={`${d.player_id}-${d.player_name}-${i}`}
          className="flex gap-2 rounded-md border border-border/60 bg-surface-hover/40 px-2 py-1.5 items-start"
        >
          {d.team_id != null && d.team_id > 0 ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img
              src={`https://www.mlbstatic.com/team-logos/${d.team_id}.svg`}
              alt=""
              width={28}
              height={28}
              className="w-7 h-7 object-contain shrink-0 mt-0.5"
            />
          ) : (
            <div
              className="w-7 h-7 shrink-0 rounded bg-surface-hover border border-border/50 mt-0.5"
              aria-hidden
            />
          )}
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
              <span className="text-xs font-semibold text-foreground">{d.player_name}</span>
              <span
                className={`text-xs font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded border ${
                  d.stat === "HR"
                    ? "border-accent/40 text-accent bg-accent-bg-active/30"
                    : d.stat === "K" || d.stat === "SV" || d.stat === "CG"
                      ? "border-info-border text-info bg-info-bg"
                      : d.stat === "2B" || d.stat === "3B"
                        ? "border-success-border text-success bg-success-bg"
                        : d.stat === "bK"
                          ? "border-warning-border text-warning bg-warning-bg"
                          : "border-border text-muted"
                }`}
              >
                {d.stat}
              </span>
            </div>
            <p className="text-sm text-muted mt-0.5 leading-snug">{milestoneKindLabel(d)}</p>
          </div>
          <div className="text-right shrink-0">
            <p className="text-xs font-mono text-foreground tabular-nums">
              {d.current}
              <span className="text-dim"> / </span>
              {d.target}
            </p>
            <p className="text-xs text-dim tabular-nums">{d.need} short</p>
          </div>
        </li>
      ))}
    </ul>
  );
}

// ── page ─────────────────────────────────────────────────────────────────────

export default async function IntelPage() {
  let list: IntelListResponse | null = null;
  let paths: SystemPathsPayload | null = null;
  let err: string | null = null;

  try {
    const [pathsRes, listRes] = await Promise.all([
      serverApiFetch("/system/paths", { cache: "no-store" }),
      serverApiFetch("/intel/snapshots?days=120&limit=1&include_body=true", { cache: "no-store" }),
    ]);
    if (pathsRes.ok) paths = (await pathsRes.json()) as SystemPathsPayload;
    if (!listRes.ok) err = await listRes.text();
    else list = (await listRes.json()) as IntelListResponse;
  } catch (e) {
    err = String(e);
  }

  return (
    <div className="p-6 flex flex-col gap-6 max-w-[1800px] mx-auto min-h-0 px-8 2xl:px-12">
      <header className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between border-b border-outline-variant pb-4">
        <div className="min-w-0 flex flex-col gap-1">
          <span className="font-mono text-xs text-accent-soft uppercase tracking-[0.2em]">
            root / intel / snapshot / current
          </span>
          <h1 className="font-headline text-3xl font-bold text-foreground">Intelligence feed</h1>
          <p className="text-xs text-muted font-mono truncate" title={paths?.intel_snapshots_dir}>
            {paths?.intel_snapshots_dir ?? "morning_intel/snapshots/"}
          </p>
          {err && (
            <p className="mt-2 border border-red-900/40 bg-red-950/20 px-3 py-2 text-xs text-red-300">
              {err.slice(0, 240)} — FastAPI unavailable
            </p>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-4 shrink-0">
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-success animate-pulse" aria-hidden />
            <span className="font-mono text-xs text-slate-400">SYS_OPERATIONAL // VER 2.4.0</span>
          </div>
          <div className="w-full sm:w-80 min-w-[200px]">
            <MorningIntelRunPanel variant="compact" />
          </div>
        </div>
      </header>

      <IntelStandoutsPanel />

      {/* Empty state */}
      {!list?.snapshots?.length && !err && (
        <div className="border border-outline-variant/30 bg-surface p-6 space-y-3 text-sm text-foreground-muted">
          <p className="text-base font-headline font-semibold text-foreground">No snapshots in the last 7 days</p>
          <ul className="list-disc pl-5 space-y-2 text-muted">
            <li>
              <strong className="text-foreground">Local:</strong>{" "}
              <code className="text-info">python morning_intel/morning_intel.py</code>
            </li>
            <li>
              <strong className="text-foreground">Drive sync:</strong>{" "}
              <code className="text-info">./scripts/pull_mlbops_from_drive.sh</code>
            </li>
          </ul>
          <p className="text-xs text-dim">
            Override path with <code className="text-info">MLB_INTEL_SNAPSHOTS_DIR</code>.
          </p>
        </div>
      )}

      {/* Snapshot articles */}
      <div className="space-y-6">
        {list?.snapshots.map((snap) => {
          const d = snap.data;

          if (snap.error) {
            return (
              <article key={snap.anchor} className="border border-red-900/40 bg-red-950/20 p-4">
                <h2 className="text-sm font-headline font-semibold text-foreground">{snap.anchor}</h2>
                <p className="text-sm text-red-300 mt-1">{snap.error}</p>
              </article>
            );
          }
          if (!d) {
            return (
              <article key={snap.anchor} className="border border-outline-variant/30 bg-surface p-4">
                <h2 className="text-sm font-headline font-semibold text-foreground">{snap.anchor}</h2>
                <Link href={`/api/backend/intel/snapshots/${snap.anchor}`} className="text-sm text-tertiary hover:underline mt-2 inline-block" target="_blank" rel="noreferrer">
                  Open JSON →
                </Link>
              </article>
            );
          }

          const pitchers = anomalyRows(d.anomalies_pitchers);
          const batters = anomalyRows(d.anomalies_batters);
          const txs = parsedTransactionsFromSnapshot(d);
          const milestones = (d.milestones as string[]) || [];
          const milestones_detail = coerceMilestoneDetail(d.milestones_detail);
          const pulse = (d.watchlist_pulse as string[]) || [];
          const pulse_detail = coerceWatchlistPulseDetail(d.watchlist_pulse_detail);
          const hasMilestones = milestoneRowCount(milestones_detail, milestones) > 0;
          const hasPulse = pulse.length > 0 || pulse_detail.length > 0;
          const hasAnomalies = pitchers.length > 0 || batters.length > 0;
          const anomalyTwoCol = pitchers.length > 0 && batters.length > 0;

          return (
            <article
              key={snap.anchor}
              className="article-card border border-outline-variant/30 bg-surface-container overflow-hidden"
            >
              <div className="px-5 py-4 border-b border-outline-variant/30 bg-surface-header flex items-center justify-between gap-4">
                <div>
                  <h2 className="text-base font-headline font-bold text-foreground leading-tight">
                    {formatAnchorHeading(snap.anchor)}
                  </h2>
                  <p className="text-xs font-mono text-muted mt-0.5">ANCHOR: {snap.anchor}</p>
                </div>
                <Link
                  href={`/api/backend/intel/snapshots/${snap.anchor}`}
                  className="shrink-0 border border-outline px-3 py-1 font-mono text-xs text-slate-400 hover:bg-accent-bg hover:text-foreground transition-colors uppercase tracking-wide"
                  target="_blank"
                  rel="noreferrer"
                >
                  RAW_JSON
                </Link>
              </div>

              {/* Anomalies — compact copy; one column if only pitchers or only batters */}
              {hasAnomalies && (
                <div
                  className={`px-4 py-3 border-b border-outline-variant/30 bg-surface-header/30 grid gap-4 ${
                    anomalyTwoCol ? "md:grid-cols-2 md:gap-5" : "grid-cols-1 max-w-xl"
                  }`}
                >
                  <p className="col-span-full text-sm text-dim leading-snug -mb-1">
                    Statcast-only flags: biggest moves in the short window vs a longer baseline (not projections).
                  </p>
                  {pitchers.length > 0 && (
                    <div className="min-w-0">
                      <SectionLabel subtle>Pitchers</SectionLabel>
                      <AnomalyList rows={pitchers} />
                    </div>
                  )}
                  {batters.length > 0 && (
                    <div className="min-w-0">
                      <SectionLabel subtle>Batters</SectionLabel>
                      <AnomalyList rows={batters} />
                    </div>
                  )}
                </div>
              )}

              {/* Body: transactions (2/3) + right rail (1/3) */}
              <div className="grid xl:grid-cols-3 divide-y xl:divide-y-0 xl:divide-x divide-outline-variant/20">
                {/* Transactions */}
                <div className="xl:col-span-2 px-5 py-4">
                  <SectionLabel>Transactions ({txs.length})</SectionLabel>
                  {txs.length === 0 ? (
                    <p className="text-sm text-dim">No transactions in this snapshot.</p>
                  ) : (
                    <div className="overflow-y-auto max-h-[540px] pr-1">
                      {txs.map((tx, i) => (
                        <TransactionRow key={i} tx={tx} />
                      ))}
                    </div>
                  )}
                </div>

                {/* Right rail */}
                <div className="px-5 py-4 space-y-6">
                  {/* Milestones */}
                  {hasMilestones && (
                    <div>
                      <SectionLabel subtle>Milestone watch</SectionLabel>
                      <p className="text-xs text-dim leading-snug mb-2">
                        Nearest clean-number marks within 10 (MLB Stats API). Rows mix stat types on purpose so this complements Leaders, not duplicate it. Pitchers use K / SV / CG, not batting HR chase lines. Re-run morning intel to refresh.
                      </p>
                      <MilestoneWatchList detail={milestones_detail} legacyLines={milestones} />
                    </div>
                  )}

                  {/* Watchlist pulse */}
                  {hasPulse && (
                    <div>
                      <SectionLabel>Watchlist pulse</SectionLabel>
                      <p className="text-xs text-dim leading-snug mb-2">
                        Last-7d Statcast snapshot plus MLB season OPS (batters) or ERA (pitchers) vs the prior year.
                      </p>
                      <WatchlistPulseList detail={pulse_detail} legacyLines={pulse} />
                    </div>
                  )}
                </div>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}
