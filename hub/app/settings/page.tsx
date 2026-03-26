"use client";

import { useEffect, useState } from "react";
import type { WatchlistPlayer } from "@/lib/db";

export default function SettingsPage() {
  const [watchlist, setWatchlist] = useState<WatchlistPlayer[]>([]);
  const [notifyStatus, setNotifyStatus] = useState<string | null>(null);
  const [notifyLoading, setNotifyLoading] = useState(false);

  useEffect(() => {
    fetch("/api/watchlist")
      .then((r) => r.json())
      .then((d) => setWatchlist(d.players || []))
      .catch(() => {});
  }, []);

  async function sendDigest() {
    setNotifyLoading(true);
    setNotifyStatus(null);
    try {
      const res = await fetch("/api/notify", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: "digest" }),
      });
      const data = await res.json();
      if (res.ok) {
        setNotifyStatus(`Digest sent! Email ID: ${data.email_id}`);
      } else {
        setNotifyStatus(`Error: ${data.error || "Unknown"}`);
      }
    } finally {
      setNotifyLoading(false);
    }
  }

  return (
    <div className="max-w-2xl mx-auto p-6 space-y-8">
      <h1 className="text-2xl font-bold text-gray-900">Settings</h1>

      {/* Notifications */}
      <section className="bg-white border border-gray-200 rounded-lg p-5">
        <h2 className="font-semibold text-gray-800 mb-3">Notifications</h2>
        <p className="text-sm text-gray-500 mb-4">
          Test that your Resend (email) and Twilio (WhatsApp) credentials are working.
        </p>
        <button
          onClick={sendDigest}
          disabled={notifyLoading}
          className="bg-[#2c7a7b] hover:bg-[#235f60] text-white text-sm font-medium px-4 py-2 rounded transition-colors disabled:opacity-50"
        >
          {notifyLoading ? "Sending…" : "Send test morning digest"}
        </button>
        {notifyStatus && (
          <p className={`mt-3 text-sm ${notifyStatus.startsWith("Error") ? "text-red-600" : "text-green-700"}`}>
            {notifyStatus}
          </p>
        )}
      </section>

      {/* Player watchlist */}
      <section className="bg-white border border-gray-200 rounded-lg p-5">
        <div className="flex items-center justify-between mb-4">
          <h2 className="font-semibold text-gray-800">Player Watchlist</h2>
          <span className="text-xs text-gray-400">{watchlist.filter((p) => p.active).length} active</span>
        </div>
        <p className="text-sm text-gray-500 mb-4">
          Managed via <code className="bg-gray-100 px-1 rounded text-xs">jobs/player_watchlist.json</code> or directly in <code className="bg-gray-100 px-1 rounded text-xs">data/hub.db</code>.
        </p>

        {watchlist.length === 0 ? (
          <p className="text-sm text-gray-400 italic">
            No players in watchlist yet. Add them to jobs/player_watchlist.json and run:
            <code className="block mt-1 bg-gray-50 rounded p-2 text-xs text-gray-600">
              python jobs/daily_card_generator.py --seed-watchlist
            </code>
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 uppercase border-b">
                  <th className="text-left py-2 pr-4">Player</th>
                  <th className="text-left py-2 pr-4">Position</th>
                  <th className="text-left py-2 pr-4">Team</th>
                  <th className="text-left py-2 pr-4">Priority</th>
                  <th className="text-left py-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {watchlist.map((p) => (
                  <tr key={p.player_id} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 pr-4 font-medium">{p.player_name}</td>
                    <td className="py-2 pr-4 text-gray-500 capitalize">{p.position || "—"}</td>
                    <td className="py-2 pr-4 text-gray-500">{p.team_abbrev || "—"}</td>
                    <td className="py-2 pr-4 text-gray-500">{p.priority}</td>
                    <td className="py-2">
                      <span className={`text-xs px-2 py-0.5 rounded-full ${p.active ? "bg-green-100 text-green-700" : "bg-gray-100 text-gray-500"}`}>
                        {p.active ? "Active" : "Paused"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* Quick reference */}
      <section className="bg-white border border-gray-200 rounded-lg p-5">
        <h2 className="font-semibold text-gray-800 mb-3">Quick Commands</h2>
        <div className="space-y-2 text-sm font-mono">
          {[
            ["Start FastAPI", "uvicorn api.main:app --port 8000 --reload"],
            ["Start hub", "cd hub && npm run dev"],
            ["Run morning digest", "python jobs/morning_digest.py"],
            ["Generate today's cards", "python jobs/daily_card_generator.py"],
            ["Run weekly report", "python jobs/weekly_report.py"],
          ].map(([label, cmd]) => (
            <div key={label}>
              <p className="text-xs text-gray-400 uppercase tracking-wide">{label}</p>
              <code className="block bg-gray-50 rounded px-3 py-1.5 text-gray-700 text-xs">
                {cmd}
              </code>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
