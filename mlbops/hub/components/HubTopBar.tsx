"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { MalliBrandMark } from "@/components/MalliBrandMark";
import { getApiBase } from "@/lib/api";

/** Subnav: real routes — LIVE = dashboard, SCHEDULE, TRANSACTIONS = intel feed. */
const SUBNAV = [
  { href: "/", label: "LIVE", match: (p: string) => p === "/" },
  { href: "/schedule", label: "SCHEDULE", match: (p: string) => p.startsWith("/schedule") },
  { href: "/intel", label: "TRANSACTIONS", match: (p: string) => p.startsWith("/intel") },
] as const;

const CRUMB_LABELS: Record<string, string> = {
  intel: "INTEL",
  schedule: "SCHEDULE",
  watchlist: "WATCHLIST",
  leaderboards: "LEADERS",
  insights: "INSIGHTS",
  cards: "CARDS",
  queue: "QUEUE",
  settings: "SETTINGS",
};

function hubBreadcrumbLeaf(pathname: string): string {
  if (pathname === "/") return "DASHBOARD";
  const seg = pathname.split("/").filter(Boolean)[0] ?? "";
  return CRUMB_LABELS[seg] ?? seg.replace(/-/g, " ").toUpperCase();
}

function PipelineReadinessLink() {
  const [overall, setOverall] = useState<"ok" | "warn" | "block" | null>(null);
  useEffect(() => {
    let cancelled = false;
    fetch(`${getApiBase()}/system/readiness`, { cache: "no-store" })
      .then((r) => (r.ok ? r.json() : null))
      .then((d: { overall?: string } | null) => {
        if (cancelled || !d?.overall) return;
        if (d.overall === "ok" || d.overall === "warn" || d.overall === "block") {
          setOverall(d.overall);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);
  if (overall === null) {
    return <span className="hidden md:inline w-7 h-2" aria-hidden />;
  }
  const dot = overall === "ok" ? "bg-success" : overall === "warn" ? "bg-warning" : "bg-danger";
  return (
    <Link
      href="/#pipeline-checklist"
      className="hidden md:flex items-center gap-2 text-[10px] font-mono uppercase tracking-widest text-slate-500 hover:text-foreground shrink-0"
      title="Pipeline readiness"
    >
      <span className={`h-2 w-2 rounded-full ${dot}`} aria-hidden />
      ops
    </Link>
  );
}

export function HubTopBar() {
  const pathname = usePathname();
  const crumbLeaf = hubBreadcrumbLeaf(pathname);

  return (
    <header className="hidden lg:flex items-center justify-between shrink-0 px-6 py-4 w-full bg-background border-b border-outline">
      <div className="flex items-center gap-6 min-w-0 flex-1">
        <Link
          href="/"
          className="flex items-center gap-3 shrink-0 pr-6 border-r border-outline-variant/60 hover:opacity-90 transition-opacity"
        >
          <MalliBrandMark className="shrink-0" />
          <span className="flex flex-col leading-tight">
            <span className="text-accent font-headline text-xl font-bold tracking-tight">
              Mallitalytics
            </span>
            <span className="font-mono text-[10px] text-slate-500 uppercase tracking-widest">
              MLB V2.0
            </span>
          </span>
        </Link>
        <p
          className="hidden xl:flex items-baseline gap-2 font-mono text-xs tracking-tight text-slate-500 shrink-0"
          aria-label={`Breadcrumb: root, ${crumbLeaf}`}
        >
          <span>ROOT</span>
          <span className="text-slate-600" aria-hidden>
            /
          </span>
          <span className="text-accent font-semibold">{crumbLeaf}</span>
        </p>
        <nav className="flex gap-6 ml-auto xl:ml-0" aria-label="Console sections">
          {SUBNAV.map(({ href, label, match }) => {
            const active = match(pathname);
            return (
              <Link
                key={href}
                href={href}
                className={`text-xs font-mono tracking-tighter pb-1 border-b-2 transition-colors ${
                  active
                    ? "text-accent border-accent"
                    : "text-slate-400 border-transparent hover:text-white"
                }`}
              >
                {label}
              </Link>
            );
          })}
        </nav>
        <PipelineReadinessLink />
      </div>
      <div className="flex items-center gap-6 shrink-0">
        <div className="relative">
          <label htmlFor="hub-cmdk-placeholder" className="sr-only">
            Search (placeholder)
          </label>
          <input
            id="hub-cmdk-placeholder"
            readOnly
            tabIndex={-1}
            className="bg-surface-lowest border-0 border-b-2 border-outline-variant focus:border-accent px-3 py-1 text-xs font-mono w-48 text-foreground placeholder:text-slate-600 cursor-default"
            placeholder="CMD+K SEARCH"
            aria-hidden
          />
        </div>
        <div className="flex gap-4">
          <span
            className="material-symbols-outlined text-slate-400 text-xl cursor-default"
            aria-hidden
          >
            notifications
          </span>
          <span
            className="material-symbols-outlined text-slate-400 text-xl cursor-default"
            aria-hidden
          >
            account_circle
          </span>
        </div>
      </div>
    </header>
  );
}
