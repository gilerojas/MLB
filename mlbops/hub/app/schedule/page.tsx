import Link from "next/link";
import { getApiBase } from "@/lib/api";

export const dynamic = "force-dynamic";

// ── types ────────────────────────────────────────────────────────────────────

interface Game {
  game_pk: number;
  away_team: string;
  away_team_id: number | null;
  home_team: string;
  home_team_id: number | null;
  away_score: number | null;
  home_score: number | null;
  away_wins: number | null;
  away_losses: number | null;
  home_wins: number | null;
  home_losses: number | null;
  away_probable: string | null;
  home_probable: string | null;
  status: string;
  game_date: string;
  venue: string | null;
  game_type: string;
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

function formatGameTime(isoDate: string): string {
  try {
    const d = new Date(isoDate);
    return d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  } catch {
    return "";
  }
}

async function fetchGames(dateStr: string): Promise<{ date: string; games: Game[] }> {
  try {
    const res = await fetch(`${getApiBase()}/schedule/${dateStr}`, { cache: "no-store" });
    if (!res.ok) throw new Error("Schedule fetch failed");
    return res.json();
  } catch {
    return { date: dateStr, games: [] };
  }
}

// ── components ───────────────────────────────────────────────────────────────

function TeamLogo({ teamId, teamName }: { teamId: number | null; teamName: string }) {
  const tile =
    "w-10 h-10 flex items-center justify-center shrink-0 border border-outline-variant bg-white/95 dark:bg-white/95";
  if (!teamId) {
    return (
      <div className={tile}>
        <span className="text-xs font-bold text-foreground-muted">
          {teamName.slice(0, 3).toUpperCase()}
        </span>
      </div>
    );
  }
  return (
    <div className={tile}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        src={`https://www.mlbstatic.com/team-logos/${teamId}.svg`}
        alt={teamName}
        width={32}
        height={32}
        className="w-8 h-8 object-contain"
      />
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  const s = status.toLowerCase();
  if (s.includes("final"))
    return (
      <span className="text-xs font-mono px-2 py-0.5 bg-slate-800 text-slate-400 border border-slate-700 uppercase">
        Final
      </span>
    );
  if (s.includes("progress") || s.includes("live"))
    return (
      <span className="text-xs font-mono px-2 py-0.5 bg-danger-bg text-danger border border-danger animate-pulse uppercase">
        Live: {status.slice(0, 18)}
      </span>
    );
  if (s.includes("warmup") || s.includes("pre"))
    return (
      <span className="text-xs font-mono px-2 py-0.5 bg-surface-hover text-slate-300 border border-slate-600 uppercase">
        Pre-Game
      </span>
    );
  return (
    <span className="text-xs font-mono px-2 py-0.5 bg-surface-hover text-dim border border-border uppercase">
      {status.slice(0, 24)}
    </span>
  );
}

function GameCard({ game }: { game: Game }) {
  const hasScore = game.away_score != null && game.home_score != null;
  const awayWon = hasScore && game.away_score! > game.home_score!;
  const homeWon = hasScore && game.home_score! > game.away_score!;
  const gameTime = formatGameTime(game.game_date);

  const divisionHint = (w: number | null, l: number | null) =>
    w != null && l != null ? `${w}-${l}` : "";

  return (
    <div className="border border-outline-variant bg-surface overflow-hidden hover:border-accent transition-all group flex flex-col">
      <div className="p-4 border-b border-outline-variant/30 flex justify-between items-center bg-surface-header">
        <StatusBadge status={game.status} />
        <span className="text-xs font-mono text-slate-500 truncate max-w-[50%] text-right">
          {game.venue || `pk_${game.game_pk}`}
        </span>
      </div>
      <div className="p-6 flex flex-col space-y-6 flex-1">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center space-x-4 min-w-0">
            <TeamLogo teamId={game.away_team_id} teamName={game.away_team} />
            <div className="min-w-0">
              <h3 className="text-sm font-bold uppercase leading-none text-foreground truncate">
                {game.away_team}
              </h3>
              <p className="text-xs font-mono text-slate-500 mt-1">
                {divisionHint(game.away_wins, game.away_losses)}
              </p>
            </div>
          </div>
          {hasScore ? (
            <span
              className={`text-3xl font-headline font-bold tabular-nums shrink-0 ${
                awayWon ? "text-foreground" : "text-slate-500"
              }`}
            >
              {game.away_score}
            </span>
          ) : (
            <span className="text-2xl font-headline font-light text-slate-600 shrink-0">--</span>
          )}
        </div>
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center space-x-4 min-w-0">
            <TeamLogo teamId={game.home_team_id} teamName={game.home_team} />
            <div className="min-w-0">
              <h3 className="text-sm font-bold uppercase leading-none text-foreground truncate">
                {game.home_team}
              </h3>
              <p className="text-xs font-mono text-slate-500 mt-1">
                {divisionHint(game.home_wins, game.home_losses)}
              </p>
            </div>
          </div>
          {hasScore ? (
            <span
              className={`text-3xl font-headline font-bold tabular-nums shrink-0 ${
                homeWon ? "text-accent-soft" : "text-slate-500"
              }`}
            >
              {game.home_score}
            </span>
          ) : (
            <span className="text-2xl font-headline font-light text-slate-600 shrink-0">--</span>
          )}
        </div>
      </div>
      <div className="mt-auto p-3 bg-surface-lowest grid grid-cols-2 gap-4 border-t border-outline-variant/50">
        <div>
          <p className="text-xs font-mono text-slate-500 uppercase tracking-widest">Pitchers</p>
          <p className="text-xs font-mono mt-1 text-foreground">
            {game.away_probable ?? "TBD"} / {game.home_probable ?? "TBD"}
          </p>
        </div>
        <div className="text-right">
          <p className="text-xs font-mono text-slate-500 uppercase tracking-widest">Metadata</p>
          <p className="text-xs font-mono mt-1 text-foreground truncate">
            {game.venue ?? "—"} | {gameTime || "—"}
          </p>
        </div>
      </div>
    </div>
  );
}

// ── page ─────────────────────────────────────────────────────────────────────

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

  // Build date tabs: yesterday, today, +1 through +4
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
            <GameCard key={game.game_pk} game={game} />
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
