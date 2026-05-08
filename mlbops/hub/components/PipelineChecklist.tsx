"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { getApiBase, secureFetch } from "@/lib/api";

type Severity = "ok" | "warn" | "block";
type CheckAction = "sync_drive" | "run_intel" | "open_settings" | "open_docs" | "ingest" | null;

type ReadinessCheck = {
  id: string;
  ok: boolean;
  severity: Severity;
  label: string;
  detail: string;
  action: CheckAction;
};

type ReadinessPayload = {
  overall: Severity;
  environment?: { repo_root: string; warehouse_dir: string; intel_snapshots_dir: string; machine_hint: string };
  checks: ReadinessCheck[];
};

function severityRowCls(sev: Severity): string {
  if (sev === "block") return "border-l-4 border-danger bg-danger-bg/20";
  if (sev === "warn") return "border-l-4 border-warning bg-warning-bg/10";
  return "border-l-4 border-success border-opacity-50 bg-surface";
}

const STORAGE_KEY = "mlbops-pipeline-checklist-collapsed";

export function PipelineChecklist() {
  const api = getApiBase();
  const router = useRouter();
  const [data, setData] = useState<ReadinessPayload | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [collapsed, setCollapsed] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    setErr(null);
    fetch(`${api}/system/readiness`, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(String(r.status));
        return r.json() as Promise<ReadinessPayload>;
      })
      .then(setData)
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false));
  }, [api]);

  useEffect(() => {
    load();
  }, [load]);

  const runIntel = useCallback(async () => {
    setBusy("intel");
    setMsg(null);
    try {
      const res = await secureFetch(`${api}/intel/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          dry_run: false,
          skip_notify: true,
          skip_claude: true,
          skip_cards: true,
        }),
      });
      const body = (await res.json()) as { detail?: unknown; ok?: boolean; returncode?: number };
      if (!res.ok) {
        setMsg(typeof body.detail === "string" ? body.detail : `HTTP ${res.status}`);
        return;
      }
      if (body.ok) {
        setMsg("Intel run finished. Refreshing…");
        router.refresh();
        load();
      } else {
        setMsg(`Intel exited with code ${body.returncode ?? "?"}. See Morning Intel panel for logs.`);
      }
    } catch (e) {
      setMsg(String(e));
    } finally {
      setBusy(null);
    }
  }, [api, router, load]);

  const syncDrive = useCallback(async () => {
    setBusy("sync");
    setMsg(null);
    try {
      const res = await secureFetch(`${api}/system/sync-drive`, { method: "POST" });
      if (res.status === 403) {
        setMsg("Sync disabled: set MLBOPS_ALLOW_INTEL_RUN=1 on the API.");
        return;
      }
      if (!res.ok || !res.body) {
        setMsg(await res.text().catch(() => "Sync failed."));
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        if (buffer.includes("__SYNC_OK__")) break;
        if (buffer.includes("__SYNC_FAIL__")) {
          setMsg("Drive sync failed — see Settings for log.");
          return;
        }
      }
      setMsg("Drive sync complete. Refreshing checklist…");
      load();
    } catch (e) {
      setMsg(String(e));
    } finally {
      setBusy(null);
    }
  }, [api, load]);

  const onAction = (a: CheckAction) => {
    if (a === "sync_drive") void syncDrive();
    else if (a === "run_intel") void runIntel();
    else if (a === "open_settings") router.push("/settings");
    else if (a === "open_docs" || a === "ingest") window.open(`${api}/docs`, "_blank", "noopener,noreferrer");
  };

  const toggleCollapsed = () => {
    setCollapsed((c) => {
      const next = !c;
      if (typeof window !== "undefined") localStorage.setItem(STORAGE_KEY, next ? "1" : "0");
      return next;
    });
  };

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (localStorage.getItem(STORAGE_KEY) === "1") setCollapsed(true);
  }, []);

  const overall = data?.overall ?? "ok";
  const titleCls =
    overall === "block"
      ? "text-danger"
      : overall === "warn"
        ? "text-warning"
        : "text-success";

  return (
    <section id="pipeline-checklist" className="mb-8 border border-outline-variant/30 bg-surface overflow-hidden">
      <button
        type="button"
        onClick={toggleCollapsed}
        className="w-full flex items-center justify-between gap-4 px-4 py-3 text-left hover:bg-surface-hover/30 transition-colors"
        aria-expanded={!collapsed}
      >
        <div className="min-w-0">
          <h3 className="font-headline font-bold text-sm uppercase tracking-tighter text-foreground">
            Pipeline readiness
          </h3>
          <p className="text-xs font-mono text-slate-500 mt-0.5 truncate">
            {data?.environment
              ? `${data.environment.machine_hint} · ${data.environment.warehouse_dir}`
              : loading
                ? "Loading…"
                : "—"}
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <span className={`text-xs font-mono font-bold uppercase ${titleCls}`}>
            {overall === "ok" ? "All clear" : overall === "warn" ? "Warnings" : "Blocked"}
          </span>
          <span className="material-symbols-outlined text-slate-500 text-lg" aria-hidden>
            {collapsed ? "expand_more" : "expand_less"}
          </span>
        </div>
      </button>

      {!collapsed && (
        <div className="px-4 pb-4 space-y-3 border-t border-outline-variant/20">
          {err && <p className="text-sm text-danger font-mono">Readiness: {err}</p>}
          {msg && <p className="text-sm text-tertiary font-mono">{msg}</p>}
          {loading && !data && <p className="text-sm text-slate-500 font-mono">Scanning mirror…</p>}
          {data?.checks && (
            <ul className="space-y-2">
              {data.checks.map((c) => (
                <li key={c.id} className={`rounded-sm px-3 py-2 text-sm ${severityRowCls(c.severity)}`}>
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="font-headline font-semibold text-foreground flex items-center gap-2">
                        <span className="material-symbols-outlined text-base shrink-0" aria-hidden>
                          {c.severity === "ok" ? "check_circle" : c.severity === "warn" ? "warning" : "error"}
                        </span>
                        {c.label}
                      </div>
                      <p className="text-xs font-mono text-slate-500 mt-1 whitespace-pre-wrap">{c.detail}</p>
                    </div>
                    {c.action && (
                      <button
                        type="button"
                        onClick={() => onAction(c.action)}
                        disabled={busy !== null}
                        className="shrink-0 px-2 py-1 text-[10px] font-mono uppercase border border-outline text-accent-soft hover:bg-accent-bg disabled:opacity-50"
                      >
                        {c.action === "sync_drive" && (busy === "sync" ? "…" : "Sync")}
                        {c.action === "run_intel" && (busy === "intel" ? "…" : "Run intel")}
                        {c.action === "open_settings" && "Settings"}
                        {(c.action === "open_docs" || c.action === "ingest") && "API docs"}
                      </button>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          )}
          <div className="flex flex-wrap items-center gap-2 pt-1">
            <button
              type="button"
              onClick={load}
              disabled={loading}
              className="text-xs font-mono uppercase text-tertiary border border-outline px-2 py-1 hover:bg-surface-hover"
            >
              {loading ? "…" : "Refresh"}
            </button>
            <Link href="/settings" className="text-xs font-mono uppercase text-tertiary hover:underline">
              Warehouse settings
            </Link>
          </div>
        </div>
      )}
    </section>
  );
}
