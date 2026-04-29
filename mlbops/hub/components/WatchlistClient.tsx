"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { getApiBase } from "@/lib/api";

type Player = {
  player_id: number;
  player_name: string;
  position: string | null;
  team_abbrev: string | null;
  active: number;
  priority: number;
  notes: string | null;
};

type PulseDetailRow = {
  player_id: number;
  player_name: string;
  pulse_summary?: string;
  recent_lines?: string[];
};

type SnapshotRow = {
  anchor: string;
  data?: {
    watchlist_pulse?: string[];
    watchlist_pulse_detail?: PulseDetailRow[];
  };
};

export default function WatchlistClient() {
  const base = getApiBase();
  const [players, setPlayers] = useState<Player[]>([]);
  const [pulseByAnchor, setPulseByAnchor] = useState<Record<string, string[]>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setMsg(null);
    try {
      const [wl, intel] = await Promise.all([
        fetch(`${base}/watchlist`).then((r) => r.json()),
        fetch(`${base}/intel/snapshots?days=120&limit=1&include_body=true`).then((r) =>
          r.json()
        ),
      ]);
      setPlayers(wl.players || []);
      const pulses: Record<string, string[]> = {};
      for (const s of (intel.snapshots || []) as SnapshotRow[]) {
        const d = s.data;
        if (d?.watchlist_pulse?.length) {
          pulses[s.anchor] = d.watchlist_pulse;
        } else if (d?.watchlist_pulse_detail?.length) {
          pulses[s.anchor] = d.watchlist_pulse_detail.map(
            (row) =>
              row.pulse_summary ||
              [row.player_name, ...(row.recent_lines || [])].filter(Boolean).join(" — "),
          );
        }
      }
      setPulseByAnchor(pulses);
    } catch (e) {
      setMsg(String(e));
    } finally {
      setLoading(false);
    }
  }, [base]);

  useEffect(() => {
    load();
  }, [load]);

  const pulsesFor = useMemo(() => {
    return (name: string) => {
      if (!name.trim()) return [];
      const lines: string[] = [];
      const keys = Object.keys(pulseByAnchor).sort().reverse();
      const needle = name.toLowerCase();
      for (const k of keys) {
        for (const line of pulseByAnchor[k] || []) {
          if (line.toLowerCase().includes(needle)) {
            lines.push(`${k}: ${line}`);
          }
        }
      }
      return lines.slice(0, 5);
    };
  }, [pulseByAnchor]);

  async function save(next: Player[]) {
    setSaving(true);
    setMsg(null);
    try {
      const res = await fetch(`${base}/watchlist`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          players: next.map((p) => ({
            player_id: p.player_id,
            player_name: p.player_name,
            position: p.position || undefined,
            team_abbrev: p.team_abbrev || undefined,
            active: p.active === 1,
            priority: p.priority,
            notes: p.notes || "",
          })),
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Save failed");
      setPlayers(data.players || []);
      setMsg("Saved to jobs/player_watchlist.json + DB.");
    } catch (e) {
      setMsg(String(e));
    } finally {
      setSaving(false);
    }
  }

  function toggleActive(i: number) {
    const next = [...players];
    next[i] = { ...next[i], active: next[i].active ? 0 : 1 };
    setPlayers(next);
  }

  function updateField(i: number, field: keyof Player, value: string | number) {
    const next = [...players];
    next[i] = { ...next[i], [field]: value } as Player;
    setPlayers(next);
  }

  function removeRow(i: number) {
    save(players.filter((_, j) => j !== i));
  }

  function addRow() {
    setPlayers([
      ...players,
      {
        player_id: 0,
        player_name: "",
        position: "batter",
        team_abbrev: "",
        active: 1,
        priority: 5,
        notes: "",
      },
    ]);
  }

  if (loading) {
    return (
      <p className="text-muted text-base p-6">Loading watchlist…</p>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button"
          onClick={() => save(players)}
          disabled={saving}
          className="rounded bg-info px-4 py-2 text-base font-medium text-white hover:opacity-90 disabled:opacity-50"
        >
          {saving ? "Saving…" : "Save"}
        </button>
        <button
          type="button"
          onClick={addRow}
          className="rounded border border-border px-4 py-2 text-base text-foreground hover:bg-surface-hover"
        >
          Add player
        </button>
        <button
          type="button"
          onClick={load}
          className="text-base text-info hover:underline"
        >
          Reload
        </button>
        {msg && (
          <span className="text-sm text-muted">{msg}</span>
        )}
      </div>

      <div className="overflow-x-auto border border-border rounded-lg">
        <table className="w-full text-left text-sm sm:text-base">
          <thead>
            <tr className="border-b border-border bg-surface text-muted">
              <th className="p-3">On</th>
              <th className="p-3">ID</th>
              <th className="p-3">Name</th>
              <th className="p-3">Tm</th>
              <th className="p-3">Pos</th>
              <th className="p-3">Pri</th>
              <th className="p-3 min-w-[160px]">Notes</th>
              <th className="p-3">Pulse</th>
              <th className="p-3" />
            </tr>
          </thead>
          <tbody>
            {players.map((p, i) => (
              <tr
                key={`${p.player_id}-${i}`}
                className="border-b border-border align-top hover:bg-surface-hover/80"
              >
                <td className="p-3">
                  <input
                    type="checkbox"
                    checked={p.active === 1}
                    onChange={() => toggleActive(i)}
                    className="accent-info"
                  />
                </td>
                <td className="p-3">
                  <input
                    className="w-24 rounded border border-border bg-background px-2 py-1.5 font-mono text-sm text-foreground"
                    value={p.player_id || ""}
                    onChange={(e) =>
                      updateField(i, "player_id", parseInt(e.target.value, 10) || 0)
                    }
                  />
                </td>
                <td className="p-3">
                  <input
                    className="w-40 sm:w-52 rounded border border-border bg-background px-2 py-1.5 text-sm text-foreground"
                    value={p.player_name}
                    onChange={(e) => updateField(i, "player_name", e.target.value)}
                  />
                </td>
                <td className="p-3">
                  <input
                    className="w-14 rounded border border-border bg-background px-2 py-1.5 text-sm text-foreground"
                    value={p.team_abbrev || ""}
                    onChange={(e) => updateField(i, "team_abbrev", e.target.value)}
                  />
                </td>
                <td className="p-3">
                  <select
                    className="rounded border border-border bg-background px-2 py-1.5 text-sm text-foreground"
                    value={p.position || "batter"}
                    onChange={(e) => updateField(i, "position", e.target.value)}
                  >
                    <option value="batter">batter</option>
                    <option value="pitcher">pitcher</option>
                    <option value="two-way">two-way</option>
                  </select>
                </td>
                <td className="p-3">
                  <input
                    type="number"
                    min={1}
                    max={10}
                    className="w-12 rounded border border-border bg-background px-2 py-1.5 text-sm text-foreground"
                    value={p.priority}
                    onChange={(e) =>
                      updateField(i, "priority", parseInt(e.target.value, 10) || 5)
                    }
                  />
                </td>
                <td className="p-3">
                  <input
                    className="w-full min-w-[140px] rounded border border-border bg-background px-2 py-1.5 text-sm text-muted"
                    value={p.notes || ""}
                    onChange={(e) => updateField(i, "notes", e.target.value)}
                  />
                </td>
                <td className="p-3 text-sm text-muted max-w-[240px]">
                  {p.player_name
                    ? pulsesFor(p.player_name).map((l, j) => (
                        <div key={j} className="truncate" title={l}>
                          {l}
                        </div>
                      ))
                    : "—"}
                </td>
                <td className="p-3">
                  <button
                    type="button"
                    onClick={() => removeRow(i)}
                    className="text-red-400 hover:underline text-sm"
                  >
                    Remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
