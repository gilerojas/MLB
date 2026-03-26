"use client";

import { useCallback, useEffect, useState } from "react";

type LeaderTab = "batting" | "pitching";

interface Leader {
  [key: string]: string | number | null;
}

const BATTING_SORT_OPTIONS = ["ops", "avg", "obp", "slg", "hr", "rbi", "sb"];
const PITCHING_SORT_OPTIONS = ["era", "k_per_9", "whip", "bb_per_9", "fip"];

export default function LeaderboardsPage() {
  const [tab, setTab] = useState<LeaderTab>("batting");
  const [sortBy, setSortBy] = useState("ops");
  const [leaders, setLeaders] = useState<Leader[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const FASTAPI = process.env.NEXT_PUBLIC_FASTAPI_URL || "http://localhost:8000";

  const fetchLeaders = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const ascending = tab === "pitching" && ["era", "whip", "bb_per_9", "fip"].includes(sortBy);
      const url =
        tab === "batting"
          ? `${FASTAPI}/leaderboards/batting?season=2025&sort_by=${sortBy}&min_pa=50&limit=25`
          : `${FASTAPI}/leaderboards/pitching?season=2025&sort_by=${sortBy}&min_ip=20&limit=25&ascending=${ascending}`;
      const res = await fetch(url);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Fetch failed");
      }
      const data = await res.json();
      const rows: Leader[] = data.leaders || [];
      setLeaders(rows);
      setColumns(rows.length > 0 ? Object.keys(rows[0]).slice(0, 12) : []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [tab, sortBy, FASTAPI]);

  useEffect(() => {
    fetchLeaders();
  }, [fetchLeaders]);

  const sortOptions = tab === "batting" ? BATTING_SORT_OPTIONS : PITCHING_SORT_OPTIONS;

  return (
    <div className="max-w-5xl mx-auto p-6">
      <h1 className="text-2xl font-bold text-gray-900 mb-1">Leaderboards</h1>
      <p className="text-gray-500 text-sm mb-6">2025 season stats from warehouse</p>

      {/* Tab + sort controls */}
      <div className="flex items-center gap-4 mb-4 flex-wrap">
        <div className="flex border border-gray-200 rounded-lg overflow-hidden">
          {(["batting", "pitching"] as LeaderTab[]).map((t) => (
            <button
              key={t}
              onClick={() => {
                setTab(t);
                setSortBy(t === "batting" ? "ops" : "era");
              }}
              className={`px-4 py-2 text-sm capitalize transition-colors ${
                tab === t
                  ? "bg-[#1a365d] text-white font-medium"
                  : "bg-white text-gray-600 hover:bg-gray-50"
              }`}
            >
              {t}
            </button>
          ))}
        </div>

        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">Sort by</label>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="border border-gray-300 rounded px-2 py-1.5 text-sm bg-white"
          >
            {sortOptions.map((opt) => (
              <option key={opt} value={opt}>
                {opt.toUpperCase()}
              </option>
            ))}
          </select>
        </div>

        <button
          onClick={fetchLeaders}
          className="text-sm text-[#2c7a7b] hover:underline"
        >
          Refresh
        </button>
      </div>

      {/* Table */}
      {loading ? (
        <p className="text-gray-400 text-center py-12">Loading…</p>
      ) : error ? (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm text-red-700">
          {error}
          <br />
          <span className="text-xs text-red-500 mt-1 block">
            Make sure the FastAPI server is running: uvicorn api.main:app --port 8000
          </span>
        </div>
      ) : leaders.length === 0 ? (
        <p className="text-gray-400 text-center py-12">No data. Run the warehouse ingestion first.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm border-collapse bg-white rounded-lg overflow-hidden shadow-sm">
            <thead>
              <tr className="bg-[#1a365d] text-white">
                <th className="py-2.5 px-3 text-left">#</th>
                {columns.map((col) => (
                  <th
                    key={col}
                    onClick={() => setSortBy(col)}
                    className={`py-2.5 px-3 text-left cursor-pointer hover:bg-[#2a4a7f] uppercase text-xs ${
                      col === sortBy ? "text-yellow-300" : ""
                    }`}
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
                  className={`${
                    i % 2 === 0 ? "bg-white" : "bg-gray-50"
                  } hover:bg-blue-50 transition-colors`}
                >
                  <td className="py-2 px-3 text-gray-400 text-xs">{i + 1}</td>
                  {columns.map((col) => (
                    <td
                      key={col}
                      className={`py-2 px-3 ${
                        col === sortBy ? "font-semibold text-[#1a365d]" : "text-gray-700"
                      }`}
                    >
                      {row[col] != null
                        ? typeof row[col] === "number"
                          ? Number(row[col]).toFixed(
                              ["avg", "ops", "obp", "slg", "era", "whip", "fip"].includes(col)
                                ? 3
                                : ["k_per_9", "bb_per_9"].includes(col)
                                ? 1
                                : 0
                            )
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
