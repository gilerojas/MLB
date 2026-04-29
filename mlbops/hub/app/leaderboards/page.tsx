"use client";

import { useCallback, useEffect, useState } from "react";
import { getApiBase } from "@/lib/api";

type LeaderTab = "batting" | "pitching";

interface Leader {
  [key: string]: string | number | null | undefined;
}

const BATTING_SORT_PRESETS = [
  "ops",
  "avg",
  "obp",
  "slg",
  "hr",
  "rbi",
  "sb",
  "barrel_pct",
  "xwoba",
  "hard_hit_pct",
  "sweet_spot_pct",
];

const PITCHING_SORT_PRESETS = [
  "era",
  "k_per_9",
  "whip",
  "bb_per_9",
  "fip",
  "k_bb_ratio",
  "whiff_pct",
  "stuff_plus",
];

export default function LeaderboardsPage() {
  const [tab, setTab] = useState<LeaderTab>("batting");
  const [season, setSeason] = useState(2026);
  const [sortBy, setSortBy] = useState("ops");
  /** Low defaults so early-season (e.g. 2026) isn’t empty; raise toward 50 PA / 20 IP for “qualified” lists. */
  const [minPa, setMinPa] = useState(1);
  const [minIp, setMinIp] = useState(0);
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const api = getApiBase();

  const fetchLeaders = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const ascending =
        tab === "pitching" &&
        ["era", "whip", "bb_per_9", "fip"].includes(sortBy);
      const url =
        tab === "batting"
          ? `${api}/leaderboards/batting?season=${season}&sort_by=${encodeURIComponent(sortBy)}&min_pa=${minPa}&limit=50`
          : `${api}/leaderboards/pitching?season=${season}&sort_by=${encodeURIComponent(sortBy)}&min_ip=${minIp}&limit=50&ascending=${ascending}`;
      const res = await fetch(url, { signal: AbortSignal.timeout(120_000) });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(
          typeof data.detail === "string" ? data.detail : "Fetch failed"
        );
      }
      const rows: Leader[] = data.leaders || [];
      setLeaders(rows);
      setColumns(
        rows.length > 0
          ? Object.keys(rows[0]).filter((c) => !c.startsWith("_"))
          : []
      );
    } catch (e) {
      const name = e instanceof Error ? e.name : "";
      const hint =
        name === "TimeoutError"
          ? "Request timed out (120s). The API may be stuck on a slow warehouse path (e.g. Google Drive). Prefer syncing to data/warehouse/mlb and unsetting MLB_WAREHOUSE_DIR."
          : String(e);
      setError(hint);
      setLeaders([]);
      setColumns([]);
    } finally {
      setLoading(false);
    }
  }, [api, tab, season, sortBy, minPa, minIp]);

  useEffect(() => {
    fetchLeaders();
  }, [fetchLeaders]);

  const sortPresets = tab === "batting" ? BATTING_SORT_PRESETS : PITCHING_SORT_PRESETS;

  function setSort(col: string) {
    setSortBy(col);
  }

  const numFmt = (col: string, v: number) => {
    if (["avg", "ops", "obp", "slg", "era", "whip", "fip", "xwoba"].includes(col)) {
      return v.toFixed(3);
    }
    if (
      ["k_per_9", "bb_per_9", "barrel_pct", "hard_hit_pct", "whiff_pct"].includes(
        col
      )
    ) {
      return v.toFixed(1);
    }
    if (Number.isInteger(v)) return String(v);
    return v.toFixed(2);
  };

  return (
    <div className="p-6 max-w-[1800px] mx-auto px-8 2xl:px-12">
      <h1 className="text-2xl font-headline font-bold text-foreground uppercase tracking-tight">Stat leaders</h1>
      <p className="text-sm text-muted mt-2 max-w-3xl">
        Boxscore-backed leaders (parquet, Drive CSV, or live raw feeds). Use higher min
        PA / IP for full-season qualified cutoffs (~50 PA, ~20 IP).
      </p>

      <div className="flex flex-wrap items-end gap-3 mt-4 mb-3">
        <div className="flex rounded border border-border overflow-hidden">
          {([2024, 2025, 2026] as const).map((y) => (
            <button
              key={y}
              type="button"
              onClick={() => setSeason(y)}
              className={`px-4 py-2 text-sm ${
                season === y
                  ? "bg-info text-white"
                  : "bg-surface text-muted hover:text-foreground"
              }`}
            >
              {y}
            </button>
          ))}
        </div>

        <div className="flex border border-border rounded overflow-hidden">
          {(["batting", "pitching"] as LeaderTab[]).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => {
                setTab(t);
                setSortBy(t === "batting" ? "ops" : "era");
              }}
              className={`px-4 py-2 text-sm capitalize ${
                tab === t
                  ? "bg-info text-white"
                  : "bg-surface text-muted"
              }`}
            >
              {t}
            </button>
          ))}
        </div>

        {tab === "batting" ? (
          <label className="text-sm text-muted flex items-center gap-2">
            min PA
            <input
              type="number"
              min={1}
              max={500}
              value={minPa}
              onChange={(e) => setMinPa(parseInt(e.target.value, 10) || 1)}
              className="w-20 rounded border border-border bg-background px-2 py-1.5 text-foreground text-sm"
            />
          </label>
        ) : (
          <label className="text-sm text-muted flex items-center gap-2">
            min IP
            <input
              type="number"
              min={0}
              max={200}
              step={0.1}
              value={minIp}
              onChange={(e) => setMinIp(Number.isFinite(parseFloat(e.target.value)) ? parseFloat(e.target.value) : 0)}
              className="w-20 rounded border border-border bg-background px-2 py-1.5 text-foreground text-sm"
            />
          </label>
        )}

        <label className="text-sm text-muted flex items-center gap-2">
          Sort
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="rounded border border-border bg-background px-2 py-1.5 text-foreground text-sm max-w-[180px]"
          >
            {sortPresets.map((opt) => (
              <option key={opt} value={opt}>
                {opt}
              </option>
            ))}
            {columns
              .filter((c) => !sortPresets.includes(c))
              .slice(0, 20)
              .map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
          </select>
        </label>

        <button
          type="button"
          onClick={fetchLeaders}
          className="text-sm text-info hover:underline"
        >
          Refresh
        </button>
      </div>

      {loading ? (
        <p className="text-dim text-center py-12 text-base">Loading…</p>
      ) : error ? (
        <div className="rounded-md border border-red-900/50 bg-red-950/30 p-5 text-base text-red-200">
          {error}
          <p className="text-sm text-red-300/80 mt-2">
            Start API + hub: from MLB repo root run ./start_hub.sh or ./scripts/start_mlbops.sh
          </p>
        </div>
      ) : leaders.length === 0 ? (
        <div className="text-dim text-center py-12 text-base space-y-2 max-w-md mx-auto">
          <p>No players pass the current filters.</p>
          <p className="text-sm text-muted">
            Early in the year, try <strong className="text-foreground-muted">min PA = 1</strong> or{" "}
            <strong className="text-foreground-muted">min IP = 0</strong>. If you still see nothing,
            sync the warehouse from Drive or confirm FastAPI can read{" "}
            <code className="text-info">data/warehouse/mlb</code>.
          </p>
        </div>
      ) : (
        <div className="overflow-x-auto border border-border rounded-lg">
          <table className="w-full text-base 2xl:text-lg text-left border-collapse">
            <thead>
              <tr className="bg-surface text-muted border-b border-border">
                <th className="py-3 px-3 sticky left-0 bg-surface z-10">#</th>
                {columns.map((col) => (
                  <th
                    key={col}
                    className={`py-3 px-3 whitespace-nowrap cursor-pointer hover:text-foreground ${
                      col === sortBy ? "text-info" : ""
                    }`}
                    onClick={() => setSort(col)}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {leaders.map((row, i) => (
                <tr
                  key={i}
                  className="border-b border-border hover:bg-surface-hover/80"
                >
                  <td className="py-2 px-3 text-dim sticky left-0 bg-background">
                    {i + 1}
                  </td>
                  {columns.map((col) => (
                    <td
                      key={col}
                      className={`py-2 px-3 whitespace-nowrap ${
                        col === sortBy ? "text-foreground font-medium" : "text-foreground-muted"
                      }`}
                    >
                      {row[col] != null
                        ? typeof row[col] === "number"
                          ? numFmt(col, row[col] as number)
                          : String(row[col])
                        : "—"}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
