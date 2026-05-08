"use client";

import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { getApiBase, secureFetch } from "@/lib/api";

type PathsInfo = { intel_run_allowed?: boolean };

export type IntelRunResponse = {
  ok: boolean;
  returncode: number;
  duration_sec: number;
  stdout_tail?: string;
  stderr_tail?: string;
  command?: string[];
};

function parseErrorDetail(data: unknown): string {
  if (data && typeof data === "object" && "detail" in data) {
    const d = (data as { detail: unknown }).detail;
    if (typeof d === "string") return d;
    if (Array.isArray(d) && d[0] && typeof d[0] === "object" && "msg" in d[0]) {
      return String((d[0] as { msg: unknown }).msg);
    }
  }
  return "Request failed";
}

export function MorningIntelRunPanel({
  variant = "full",
}: {
  variant?: "full" | "compact" | "dashboard";
}) {
  const router = useRouter();
  const [allowed, setAllowed] = useState<boolean | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [logsOpen, setLogsOpen] = useState(false);
  const [lastResult, setLastResult] = useState<IntelRunResponse | null>(null);
  const [fullPipeline, setFullPipeline] = useState(false);

  useEffect(() => {
    fetch(`${getApiBase()}/system/paths`, { cache: "no-store" })
      .then((r) => r.json())
      .then((d: PathsInfo) => setAllowed(d.intel_run_allowed === true))
      .catch(() => setAllowed(false));
  }, []);

  const run = useCallback(async () => {
    setBusy(true);
    setMsg(null);
    setLastResult(null);
    try {
      const body = {
        dry_run: false,
        skip_notify: !fullPipeline,
        skip_claude: !fullPipeline,
        skip_cards: !fullPipeline,
      };
      const res = await secureFetch(`${getApiBase()}/intel/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(body),
      });
      const data = (await res.json()) as IntelRunResponse & { detail?: unknown };
      if (!res.ok) {
        setMsg(parseErrorDetail(data));
        return;
      }
      setLastResult(data);
      if (data.ok) {
        setMsg(`Snapshot updated · ${data.duration_sec}s`);
        router.refresh();
      } else {
        setMsg(`Exited with code ${data.returncode}`);
        setLogsOpen(true);
      }
    } catch (e) {
      setMsg(String(e));
    } finally {
      setBusy(false);
    }
  }, [fullPipeline, router]);

  const runId = useMemo(() => {
    if (busy) return "ID: RUN_ACTIVE";
    if (lastResult != null) return `ID: RC_${lastResult.returncode}_${lastResult.duration_sec}s`;
    return "ID: STANDBY";
  }, [busy, lastResult]);

  const cpuPct = busy ? 44 : lastResult?.ok ? 92 : 68;

  const logBlock =
    lastResult && (lastResult.stdout_tail || lastResult.stderr_tail) ? (
      <div className="mt-3 space-y-2 text-left">
        <button
          type="button"
          onClick={() => setLogsOpen((o) => !o)}
          className="text-xs font-mono font-medium text-tertiary hover:underline"
        >
          {logsOpen ? "Hide" : "Show"} script output
        </button>
        {logsOpen && (
          <pre className="max-h-48 overflow-auto border border-outline-variant/30 bg-surface-lowest p-3 text-sm leading-snug text-muted font-mono whitespace-pre-wrap break-all">
            {lastResult.stderr_tail ? (
              <>
                <span className="text-warning">stderr</span>
                {"\n"}
                {lastResult.stderr_tail}
                {"\n\n"}
              </>
            ) : null}
            {lastResult.stdout_tail ? (
              <>
                <span className="text-tertiary">stdout</span>
                {"\n"}
                {lastResult.stdout_tail}
              </>
            ) : null}
          </pre>
        )}
      </div>
    ) : null;

  const controls = (
    <>
      <p
        className={`text-foreground-muted leading-snug ${variant === "compact" ? "text-xs" : "text-sm"}`}
      >
        Regenerate{" "}
        <code className="text-muted text-sm font-mono">intel_*.json</code> on this machine. Default skips
        notify, Claude, and cards.
      </p>
      <label className="mt-3 flex cursor-pointer items-center gap-2 text-sm text-muted select-none">
        <input
          type="checkbox"
          checked={fullPipeline}
          onChange={(e) => setFullPipeline(e.target.checked)}
          className="h-4 w-4 border border-outline-variant bg-surface-lowest text-tertiary accent-tertiary focus:ring-tertiary"
        />
        <span>Full pipeline (notify + Claude + cards)</span>
      </label>
      <div className="mt-3 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={busy}
          onClick={() => void run()}
          className="inline-flex items-center justify-center bg-tertiary-container px-5 py-2.5 text-sm font-headline font-semibold text-[#002e4a] transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {busy ? (
            <span className="inline-flex items-center gap-2">
              <span
                className="h-4 w-4 animate-spin border-2 border-[#002e4a]/30 border-t-[#002e4a]"
                aria-hidden
              />
              Running…
            </span>
          ) : (
            "Regenerate snapshot"
          )}
        </button>
      </div>
      {msg && (
        <p
          className={`mt-3 text-sm font-mono ${msg.includes("disabled") || msg.includes("Exited") ? "text-warning" : "text-success"}`}
        >
          {msg}
        </p>
      )}
      {logBlock}
    </>
  );

  if (allowed === null) {
    return (
      <div className="border border-outline-variant/40 bg-surface-lowest p-4 text-sm text-muted font-mono">
        Checking API permissions…
      </div>
    );
  }
  if (!allowed) {
    return (
      <div className="border border-warning-border/50 bg-surface-lowest p-4 text-sm text-foreground leading-relaxed">
        <p className="font-headline font-medium text-foreground mb-1">Regenerate disabled</p>
        <p className="text-muted">
          Set{" "}
          <code className="bg-surface px-1.5 py-0.5 text-muted text-xs font-mono border border-outline-variant/30">
            MLBOPS_ALLOW_INTEL_RUN=1
          </code>{" "}
          on the FastAPI process.{" "}
          <code className="bg-surface px-1.5 py-0.5 text-muted text-xs font-mono border border-outline-variant/30">
            ./start_hub.sh
          </code>{" "}
          exports this by default.
        </p>
      </div>
    );
  }

  if (variant === "dashboard") {
    return (
      <div className="border border-outline-variant/40 bg-surface-lowest overflow-hidden">
        <div className="bg-surface-header px-4 py-2 border-b border-outline-variant/40 flex flex-wrap justify-between items-center gap-2">
          <div className="flex items-center gap-3 min-w-0">
            <span className="text-xs font-mono font-bold bg-accent text-[#552000] px-1 shrink-0">
              INTEL_RUN_BETA
            </span>
            <span className="text-xs font-headline font-medium uppercase tracking-tight text-foreground truncate">
              Morning Intelligence Pipeline
            </span>
          </div>
          <span className="text-xs font-mono text-slate-500 shrink-0">{runId}</span>
        </div>
        <div className="p-3 flex flex-col lg:flex-row gap-6 items-start">
          <div className="flex-1 min-w-0 w-full">{controls}</div>
          <div className="w-full lg:w-1/4 border-t lg:border-t-0 lg:border-l border-outline-variant/20 lg:pl-6 py-1">
            <div className="text-xs font-mono text-slate-500 uppercase mb-2">Node Efficiency</div>
            <div className="space-y-3">
              <div className="relative h-1 bg-surface-header w-full">
                <div
                  className="absolute h-full bg-success transition-all duration-300"
                  style={{ width: `${cpuPct}%` }}
                />
              </div>
              <div className="flex justify-between font-mono text-xs text-foreground">
                <span>CPU_USAGE</span>
                <span>{cpuPct}%</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (variant === "compact") {
    return (
      <div className="border border-outline-variant/40 bg-surface-lowest overflow-hidden">
        <div className="bg-surface-header px-3 py-2 border-b border-outline-variant/40 flex justify-between items-center gap-2">
          <span className="text-xs font-mono font-bold text-accent-soft uppercase tracking-tight">
            Morning intel
          </span>
          <span className="text-xs font-mono text-slate-500 truncate">{runId}</span>
        </div>
        <div className="p-3">{controls}</div>
      </div>
    );
  }

  return (
    <div className="border border-outline-variant/40 bg-surface-lowest overflow-hidden">
      <div className="bg-surface-header px-4 py-2 border-b border-outline-variant/40">
        <span className="text-xs font-headline font-semibold uppercase tracking-tight text-foreground">
          Morning intel
        </span>
      </div>
      <div className="p-4 sm:p-5">{controls}</div>
    </div>
  );
}
