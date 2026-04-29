"use client";

import { useState } from "react";
import { getApiBase } from "@/lib/api";

export type InsightQueuePayload = {
  title: string;
  tweet_text: string;
  game_date: string;
  season: number;
  meta: Record<string, unknown>;
};

export function InsightQueueButton({
  disabled,
  buildPayload,
}: {
  disabled: boolean;
  buildPayload: () => InsightQueuePayload;
}) {
  const [busy, setBusy] = useState(false);
  const [hint, setHint] = useState<string | null>(null);

  async function onEnqueue() {
    setBusy(true);
    setHint(null);
    try {
      const api = getApiBase();
      const body = buildPayload();
      const res = await fetch(`${api}/queue/insight-draft`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const raw = await res.text();
      let data: { detail?: unknown; id?: number } = {};
      if (raw) {
        try {
          data = JSON.parse(raw) as typeof data;
        } catch {
          data = { detail: raw.slice(0, 300) };
        }
      }
      if (!res.ok) {
        const d = data.detail;
        const msg =
          typeof d === "string"
            ? d
            : "Failed to add to queue.";
        setHint(msg);
        return;
      }
      setHint("In queue");
      window.setTimeout(() => setHint(null), 2200);
    } catch (e) {
      setHint(String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex flex-col items-end gap-0.5 shrink-0">
      <button
        type="button"
        disabled={disabled || busy}
        onClick={() => void onEnqueue()}
        className="px-2 py-0.5 rounded border border-border text-xs font-semibold uppercase tracking-wide text-dim hover:text-accent hover:border-accent/35 disabled:opacity-40 transition-colors"
      >
        {busy ? "…" : "Tweet"}
      </button>
      {hint && (
        <span
          className={`text-xs max-w-[7rem] text-right leading-tight ${hint === "In queue" ? "text-success" : "text-danger"}`}
        >
          {hint}
        </span>
      )}
    </div>
  );
}
