import Link from "next/link";
import { serverApiFetch } from "@/lib/server-api";
import { ScheduleGameCard, type ScheduleGame } from "@/components/ScheduleGameCard";

export const dynamic = "force-dynamic";

function addDays(dateStr: string, days: number): string {
  const d = new Date(dateStr + "T00:00:00");
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

function formatDisplayDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
}

function formatScheduleHeaderDate(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" }).toUpperCase();
}

async function fetchGames(dateStr: string): Promise<{ date: string; games: ScheduleGame[] }> {
  try {
    const res = await serverApiFetch(`/schedule/${dateStr}`, { cache: "no-store" });
    if (!res.ok) throw new Error("Schedule fetch failed");
    return res.json();
  } catch {
    return { date: dateStr, games: [] };
  }
}

export default async function SchedulePage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string }>;
}) {
  const params = await searchParams;
  const today = new Date().toISOString().slice(0, 10);
  const activeDate = params.date ?? today;

  const { games } = await fetchGames(activeDate);
  const final = games.filter((g) => g.status.toLowerCase().includes("final")).length;
  const live = games.filter(
    (g) => g.status.toLowerCase().includes("progress") || g.status.toLowerCase().includes("live"),
  ).length;

  const dateTabs = [-1, 0, 1, 2, 3, 4].map((offset) => {
    const d = addDays(today, offset);
    const isToday = d === today;
    const label = isToday ? "Today" : formatDisplayDate(d);
    return { date: d, label };
  });

  return (
    <div className="p-6 max-w-[1800px] mx-auto min-h-0 px-8 2xl:px-12">
      <section className="mb-8 flex flex-col md:flex-row md:items-end justify-between gap-4">
        <div>
          <h1 className="text-4xl font-headline font-bold uppercase tracking-tighter text-foreground mb-1">
            Schedule
          </h1>
          <div className="flex items-center text-sm font-mono text-outline gap-1">
            <span className="material-symbols-outlined text-sm" aria-hidden>
              calendar_today
            </span>
            <span>{formatScheduleHeaderDate(activeDate)}</span>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {live > 0 && (
            <div className="bg-surface-header px-3 py-1.5 border-l-4 border-danger flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-danger animate-pulse" aria-hidden />
              <span className="text-xs font-mono text-danger font-bold uppercase">{live} LIVE</span>
            </div>
          )}
          <div className="bg-surface-header px-3 py-1.5 border-l-4 border-outline flex items-center">
            <span className="text-xs font-mono text-foreground font-bold uppercase">{final} FINAL</span>
          </div>
          <div className="bg-surface-header px-3 py-1.5 border-l-4 border-tertiary-container flex items-center">
            <span className="text-xs font-mono text-tertiary-container font-bold uppercase">
              {games.length} GAMES TOTAL
            </span>
          </div>
        </div>
      </section>

      <section className="mb-8 border-b border-outline-variant flex overflow-x-auto no-scrollbar">
        {dateTabs.map(({ date, label }) => {
          const isActive = date === activeDate;
          const isTodayTab = date === today;
          const tabLabel = isTodayTab
            ? `${formatDisplayDate(date).toUpperCase().replace(",", "")} / TODAY`
            : formatDisplayDate(date).toUpperCase();
          return (
            <Link
              key={date}
              href={`/schedule?date=${date}`}
              className={`px-6 py-4 text-xs font-mono uppercase transition-all shrink-0 border-b-2 ${
                isActive
                  ? "text-tertiary-container font-bold border-tertiary-container bg-tertiary-container/5"
                  : "text-slate-500 border-transparent hover:text-foreground"
              }`}
            >
              {tabLabel}
            </Link>
          );
        })}
      </section>

      {games.length === 0 ? (
        <div className="text-center py-16 text-dim border border-outline-variant/30 bg-surface p-8">
          <p className="text-base font-headline">No games scheduled for this date.</p>
          <p className="text-xs font-mono mt-2">Ensure FastAPI is running.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-2 2xl:grid-cols-3 gap-6">
          {games.map((game) => (
            <ScheduleGameCard key={game.game_pk} game={game} />
          ))}
        </div>
      )}

      <div className="mt-12 border-t border-outline-variant pt-6 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
        <div className="text-xs font-mono text-slate-500 uppercase">
          System Status: <span className="text-accent">Operational</span> / {games.length} games loaded
        </div>
        <div className="flex flex-wrap gap-2">
          <Link
            href="/"
            className="bg-surface-header border border-outline-variant px-4 py-2 text-xs font-mono uppercase hover:bg-surface-hover transition-colors text-foreground"
          >
            Dashboard
          </Link>
          <Link
            href="/intel"
            className="bg-accent text-[#552000] px-4 py-2 text-xs font-mono uppercase font-bold hover:brightness-110 transition-all"
          >
            Intel feed
          </Link>
        </div>
      </div>
    </div>
  );
}
