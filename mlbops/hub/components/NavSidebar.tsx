"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { ThemeToggle } from "@/components/ThemeToggle";

const NAV_MAIN: { href: string; label: string; icon: string }[] = [
  { href: "/", label: "Dashboard", icon: "dashboard" },
  { href: "/live", label: "Live", icon: "sports_baseball" },
  { href: "/intel", label: "Intel", icon: "analytics" },
  { href: "/watchlist", label: "Watchlist", icon: "bookmarks" },
  { href: "/leaderboards", label: "Leaders", icon: "leaderboard" },
  { href: "/insights", label: "Insights", icon: "query_stats" },
  { href: "/schedule", label: "Schedule", icon: "calendar_today" },
  { href: "/cards", label: "Cards", icon: "style" },
  { href: "/queue", label: "Queue", icon: "send_and_archive" },
];

const NAV_BOTTOM = [{ href: "/settings", label: "Settings", icon: "settings" as const }];

function isActive(pathname: string, href: string) {
  if (href === "/") return pathname === "/";
  return pathname.startsWith(href);
}

export function NavSidebar() {
  const pathname = usePathname();

  return (
    <>
      {/* Desktop: fixed Stitch sidebar */}
      <aside className="hidden lg:flex flex-col fixed left-0 top-0 z-50 h-screen w-[176px] shrink-0 border-r border-outline/20 bg-surface">
        <nav className="flex-1 px-0 pt-8 mt-0 overflow-y-auto no-scrollbar">
          {NAV_MAIN.map(({ href, label, icon }) => {
            const active = isActive(pathname, href);
            return (
              <Link
                key={href}
                href={href}
                className={`flex items-center px-4 py-3 transition-all ${
                  active
                    ? "text-accent-soft border-l-2 border-accent bg-accent-bg font-bold active:scale-[0.98]"
                    : "text-slate-400 font-normal hover:bg-surface-hover hover:text-white border-l-2 border-transparent"
                }`}
              >
                <span className="material-symbols-outlined mr-3 text-[20px] shrink-0" aria-hidden>
                  {icon}
                </span>
                <span className="text-sm font-headline uppercase tracking-tight leading-tight">
                  {label}
                </span>
              </Link>
            );
          })}
        </nav>
        <div className="p-0 border-t border-outline-variant/20 mt-auto">
          {NAV_BOTTOM.map(({ href, label, icon }) => {
            const active = isActive(pathname, href);
            return (
              <Link
                key={href}
                href={href}
                className={`flex items-center gap-3 py-2 px-4 transition-colors ${
                  active
                    ? "text-accent-soft bg-accent-bg border-l-2 border-accent"
                    : "text-slate-400 hover:text-white border-l-2 border-transparent"
                }`}
              >
                <span className="material-symbols-outlined text-[18px] shrink-0" aria-hidden>
                  {icon}
                </span>
                <span className="text-sm font-headline uppercase tracking-widest">{label}</span>
              </Link>
            );
          })}
          <ThemeToggle />
        </div>
      </aside>

      {/* Mobile top bar */}
      <header className="lg:hidden sticky top-0 z-30 border-b border-border bg-background">
        <div className="px-4 py-3 flex items-center justify-between gap-2">
          <Link
            href="/"
            className="text-sm font-bold text-accent shrink-0 font-headline uppercase tracking-tight"
          >
            Mallitalytics
          </Link>
          <ThemeToggle compact />
        </div>
        <nav className="flex gap-0.5 px-2 pb-2 overflow-x-auto no-scrollbar">
          {[...NAV_MAIN, ...NAV_BOTTOM].map(({ href, label }) => (
            <Link
              key={href}
              href={href}
              className={`shrink-0 px-3 py-1.5 text-xs font-medium font-headline uppercase tracking-tight transition-colors border-b-2 ${
                isActive(pathname, href)
                  ? "text-accent-soft border-accent bg-accent-bg"
                  : "text-muted hover:text-foreground border-transparent"
              }`}
            >
              {label}
            </Link>
          ))}
        </nav>
      </header>
    </>
  );
}
