"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";
import { getApiBase, secureFetch } from "@/lib/api";
import type { WatchlistPlayer } from "@/lib/db";

export default function SettingsPage() {
  const [watchlist, setWatchlist] = useState<WatchlistPlayer[]>([]);
  const [notifyStatus, setNotifyStatus] = useState<string | null>(null);
  const [notifyLoading, setNotifyLoading] = useState(false);
  const [syncStatus, setSyncStatus] = useState<{ ok: boolean; syncedAt?: string } | null>(null);
  const [syncLoading, setSyncLoading] = useState(false);
  const [syncLines, setSyncLines] = useState<string[]>([]);
  const logEndRef = useRef<HTMLDivElement>(null);
  const api = getApiBase();

  useEffect(() => {
    fetch(`${api}/watchlist`)
      .then((r) => r.json())
      .then((d) => setWatchlist(d.players || []))
      .catch(() => {});
  }, [api]);

  async function sendDigest() {
    setNotifyLoading(true);
    setNotifyStatus(null);
    try {
      const res = await secureFetch("/api/notify", {
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

  async function syncDrive() {
    setSyncLoading(true);
    setSyncStatus(null);
    setSyncLines([]);
    try {
      const res = await secureFetch(`${api}/system/sync-drive`, { method: "POST" });
      if (!res.ok || !res.body) {
        const text = await res.text().catch(() => "Sync failed.");
        setSyncStatus({ ok: false });
        setSyncLines([text]);
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n");
        buffer = parts.pop() ?? "";
        for (const line of parts) {
          if (line.startsWith("__SYNC_OK__")) {
            const ts = line.split(" ")[1] ?? undefined;
            setSyncStatus({ ok: true, syncedAt: ts });
          } else if (line.startsWith("__SYNC_FAIL__")) {
            setSyncStatus({ ok: false });
          } else if (line.trim()) {
            setSyncLines((prev) => [...prev, line]);
            setTimeout(() => logEndRef.current?.scrollIntoView({ behavior: "smooth" }), 20);
          }
        }
      }
    } catch (e) {
      setSyncStatus({ ok: false });
      setSyncLines([String(e)]);
    } finally {
      setSyncLoading(false);
    }
  }

  return (
    <div className="p-6 max-w-2xl space-y-5 mx-auto">
      <h1 className="text-2xl font-headline font-bold text-foreground uppercase tracking-tight">Settings</h1>

      {/* Drive sync */}
      <section className="border border-outline-variant/30 bg-surface p-6">
        <h2 className="font-medium text-lg text-foreground mb-1">Warehouse sync</h2>
        <p className="text-sm text-muted mb-4">
          Pull the latest parquets, raw feeds, and intel snapshots from Google Drive via rclone.
          Requires rclone configured locally.
        </p>
        <button
          type="button"
          onClick={syncDrive}
          disabled={syncLoading}
          className="bg-info hover:opacity-90 text-white text-sm font-medium px-5 py-2.5 rounded transition-colors disabled:opacity-50"
        >
          {syncLoading ? "Syncing from Drive…" : "Sync from Google Drive"}
        </button>
        {(syncLoading || syncLines.length > 0 || syncStatus) && (
          <div className="mt-4 space-y-2">
            {syncStatus && (
              <p className={`text-sm font-medium ${syncStatus.ok ? "text-emerald-300" : "text-red-300"}`}>
                {syncStatus.ok
                  ? `Sync complete${syncStatus.syncedAt ? ` — ${syncStatus.syncedAt.slice(0, 16).replace("T", " ")}Z` : ""}`
                  : "Sync failed — see log below"}
              </p>
            )}
            {syncLines.length > 0 && (
              <div className="rounded border border-border bg-background max-h-64 overflow-y-auto p-3">
                {syncLines.map((line, i) => (
                  <p key={i} className="font-mono text-xs text-muted leading-relaxed whitespace-pre-wrap">
                    {line}
                  </p>
                ))}
                {syncLoading && (
                  <p className="font-mono text-xs text-info animate-pulse mt-1">running…</p>
                )}
                <div ref={logEndRef} />
              </div>
            )}
          </div>
        )}
      </section>

      <section className="border border-outline-variant/30 bg-surface p-6">
        <h2 className="font-medium text-lg text-foreground mb-2">Notifications</h2>
        <p className="text-sm text-muted mb-4">
          Test Resend (email) and Twilio (WhatsApp) credentials.
        </p>
        <button
          type="button"
          onClick={sendDigest}
          disabled={notifyLoading}
          className="bg-info hover:opacity-90 text-white text-base font-medium px-5 py-2.5 rounded transition-colors disabled:opacity-50"
        >
          {notifyLoading ? "Sending…" : "Send test morning digest"}
        </button>
        {notifyStatus && (
          <p
            className={`mt-3 text-base ${
              notifyStatus.startsWith("Error")
                ? "text-red-300"
                : "text-emerald-300"
            }`}
          >
            {notifyStatus}
          </p>
        )}
      </section>

      <section className="border border-outline-variant/30 bg-surface p-6">
        <div className="flex items-center justify-between mb-3">
          <h2 className="font-medium text-lg text-foreground">Player watchlist</h2>
          <Link
            href="/watchlist"
            className="text-sm text-info hover:underline"
          >
            Edit on Watchlist page
          </Link>
        </div>
        <p className="text-sm text-muted mb-4">
          Read-only preview. Changes:{" "}
          <Link href="/watchlist" className="text-info hover:underline">
            /watchlist
          </Link>{" "}
          (writes JSON + DB).
        </p>

        {watchlist.length === 0 ? (
          <p className="text-base text-dim">
            No players loaded — is FastAPI running?
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-muted border-b border-border">
                  <th className="text-left py-2.5 pr-3">Player</th>
                  <th className="text-left py-2.5 pr-3">Pos</th>
                  <th className="text-left py-2.5 pr-3">Tm</th>
                  <th className="text-left py-2.5 pr-3">Pri</th>
                  <th className="text-left py-2.5">Status</th>
                </tr>
              </thead>
              <tbody>
                {watchlist.map((p) => (
                  <tr
                    key={p.player_id}
                    className="border-b border-border text-foreground-muted"
                  >
                    <td className="py-2 pr-3 font-medium text-foreground">
                      {p.player_name}
                    </td>
                    <td className="py-2 pr-3 capitalize">{p.position || "—"}</td>
                    <td className="py-2 pr-3">{p.team_abbrev || "—"}</td>
                    <td className="py-2 pr-3">{p.priority}</td>
                    <td className="py-2">
                      <span
                        className={`text-xs px-2.5 py-0.5 rounded-full ${
                          p.active
                            ? "bg-emerald-900/40 text-emerald-200"
                            : "bg-border text-muted"
                        }`}
                      >
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

      <section className="border border-outline-variant/30 bg-surface p-6">
        <h2 className="font-medium text-lg text-foreground mb-3">Quick commands</h2>
        <div className="space-y-3 text-sm font-mono text-muted">
          {[
            ["mlbops", "./start_hub.sh or ./scripts/start_mlbops.sh (repo root)"],
            ["Hub", "cd mlbops/hub && npm run dev"],
            [
              "Morning intel",
              "python morning_intel/morning_intel.py — or Regenerate on Briefing/Intel (API needs MLBOPS_ALLOW_INTEL_RUN=1; start_hub.sh sets this)",
            ],
            ["Daily cards", "python jobs/daily_card_generator.py"],
          ].map(([label, cmd]) => (
            <div key={label}>
              <p className="text-xs text-dim uppercase tracking-wide">{label}</p>
              <code className="block bg-background rounded px-3 py-2 text-sm border border-border">
                {cmd}
              </code>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
