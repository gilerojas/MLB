"use client";

import { useCallback, useState } from "react";

export interface ScheduleGame {
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

interface BoxscoreInning {
  num: number;
  away: number | null;
  home: number | null;
}

interface BoxscorePayload {
  away_abbrev?: string;
  home_abbrev?: string;
  detailed_state?: string;
  linescore: {
    innings: BoxscoreInning[];
    away: { runs?: number; hits?: number; errors?: number };
    home: { runs?: number; hits?: number; errors?: number };
  };
  away_batting: { name: string; line: string }[];
  home_batting: { name: string; line: string }[];
  away_pitching: { name: string; line: string }[];
  home_pitching: { name: string; line: string }[];
}

function formatGameTime(isoDate: string): string {
  try {
    const d = new Date(isoDate);
    return d.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZoneName: "short" });
  } catch {
    return "";
  }
}

function TeamLogo({ teamId, teamName }: { teamId: number | null; teamName: string }) {
  const tile =
    "w-10 h-10 flex items-center justify-center shrink-0 border border-outline-variant bg-white/95 dark:bg-white/95";
  if (!teamId) {
    return (
      <div className={tile}>
        <span className="text-xs font-bold text-foreground-muted">{teamName.slice(0, 3).toUpperCase()}</span>
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

function LinescoreTable({
  awayLabel,
  homeLabel,
  linescore,
}: {
  awayLabel: string;
  homeLabel: string;
  linescore: BoxscorePayload["linescore"];
}) {
  const innings = linescore.innings;
  if (!innings.length) return null;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs font-mono border-collapse">
        <thead>
          <tr className="text-slate-500">
            <th className="text-left py-1 pr-2 font-normal"> </th>
            {innings.map((inn) => (
              <th key={inn.num} className="px-1 py-1 font-normal tabular-nums">
                {inn.num}
              </th>
            ))}
            <th className="px-1 py-1 font-bold">R</th>
            <th className="px-1 py-1 font-bold">H</th>
            <th className="px-1 py-1 font-bold">E</th>
          </tr>
        </thead>
        <tbody className="text-foreground">
          <tr>
            <td className="py-1 pr-2 text-slate-400">{awayLabel}</td>
            {innings.map((inn) => (
              <td key={`a-${inn.num}`} className="px-1 py-1 text-center tabular-nums">
                {inn.away ?? "-"}
              </td>
            ))}
            <td className="px-1 py-1 text-center font-bold tabular-nums">{linescore.away.runs ?? "-"}</td>
            <td className="px-1 py-1 text-center tabular-nums">{linescore.away.hits ?? "-"}</td>
            <td className="px-1 py-1 text-center tabular-nums">{linescore.away.errors ?? "-"}</td>
          </tr>
          <tr>
            <td className="py-1 pr-2 text-slate-400">{homeLabel}</td>
            {innings.map((inn) => (
              <td key={`h-${inn.num}`} className="px-1 py-1 text-center tabular-nums">
                {inn.home ?? "-"}
              </td>
            ))}
            <td className="px-1 py-1 text-center font-bold tabular-nums">{linescore.home.runs ?? "-"}</td>
            <td className="px-1 py-1 text-center tabular-nums">{linescore.home.hits ?? "-"}</td>
            <td className="px-1 py-1 text-center tabular-nums">{linescore.home.errors ?? "-"}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function PlayerLines({ title, rows }: { title: string; rows: { name: string; line: string }[] }) {
  if (!rows.length) return null;
  return (
    <div>
      <p className="text-xs font-mono text-slate-500 uppercase tracking-widest mb-2">{title}</p>
      <ul className="space-y-1">
        {rows.map((row) => (
          <li key={row.name} className="text-xs">
            <span className="font-medium text-foreground">{row.name}</span>
            <span className="text-slate-500 font-mono ml-2">{row.line}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function ScheduleGameCard({ game }: { game: ScheduleGame }) {
  const hasScore = game.away_score != null && game.home_score != null;
  const awayWon = hasScore && game.away_score! > game.home_score!;
  const homeWon = hasScore && game.home_score! > game.away_score!;
  const gameTime = formatGameTime(game.game_date);
  const statusLower = game.status.toLowerCase();
  const boxscoreAvailable =
    statusLower.includes("final") || statusLower.includes("progress") || statusLower.includes("live");

  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [boxscore, setBoxscore] = useState<BoxscorePayload | null>(null);

  const divisionHint = (w: number | null, l: number | null) => (w != null && l != null ? `${w}-${l}` : "");

  const loadBoxscore = useCallback(async () => {
    if (boxscore) return boxscore;
    setLoading(true);
    setError("");
    try {
      const res = await fetch(`/api/backend/schedule/boxscore/${game.game_pk}`, { cache: "no-store" });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(typeof data.detail === "string" ? data.detail : "Boxscore unavailable.");
      }
      setBoxscore(data as BoxscorePayload);
      return data as BoxscorePayload;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Boxscore unavailable.");
      return null;
    } finally {
      setLoading(false);
    }
  }, [boxscore, game.game_pk]);

  async function onScoresClick() {
    if (!boxscoreAvailable) return;
    if (!open) await loadBoxscore();
    setOpen((v) => !v);
  }

  return (
    <div className="border border-outline-variant bg-surface overflow-hidden hover:border-accent transition-all group flex flex-col">
      <div className="p-4 border-b border-outline-variant/30 flex justify-between items-center bg-surface-header">
        <StatusBadge status={game.status} />
        <span className="text-xs font-mono text-slate-500 truncate max-w-[50%] text-right">
          {game.venue || `pk_${game.game_pk}`}
        </span>
      </div>

      <button
        type="button"
        onClick={onScoresClick}
        disabled={!boxscoreAvailable}
        aria-expanded={open}
        className={`p-6 flex flex-col space-y-6 flex-1 text-left w-full transition-colors ${
          boxscoreAvailable
            ? "cursor-pointer hover:bg-surface-hover/40 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent"
            : "cursor-default"
        }`}
      >
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center space-x-4 min-w-0">
            <TeamLogo teamId={game.away_team_id} teamName={game.away_team} />
            <div className="min-w-0">
              <h3 className="text-sm font-bold uppercase leading-none text-foreground truncate">{game.away_team}</h3>
              <p className="text-xs font-mono text-slate-500 mt-1">{divisionHint(game.away_wins, game.away_losses)}</p>
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
              <h3 className="text-sm font-bold uppercase leading-none text-foreground truncate">{game.home_team}</h3>
              <p className="text-xs font-mono text-slate-500 mt-1">{divisionHint(game.home_wins, game.home_losses)}</p>
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
        {boxscoreAvailable && (
          <p className="text-xs font-mono text-accent uppercase tracking-widest">
            {open ? "Hide boxscore" : "Show boxscore"} {loading ? "..." : "↕"}
          </p>
        )}
      </button>

      {open && (
        <div className="px-6 pb-6 border-t border-outline-variant/30 bg-surface-lowest space-y-4">
          {loading && <p className="text-xs font-mono text-slate-500 pt-4">Loading boxscore...</p>}
          {error && <p className="text-xs font-mono text-danger pt-4">{error}</p>}
          {boxscore && !loading && (
            <>
              <div className="pt-4">
                <LinescoreTable
                  awayLabel={boxscore.away_abbrev || game.away_team.slice(0, 3).toUpperCase()}
                  homeLabel={boxscore.home_abbrev || game.home_team.slice(0, 3).toUpperCase()}
                  linescore={boxscore.linescore}
                />
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <PlayerLines title="Away batting" rows={boxscore.away_batting} />
                <PlayerLines title="Home batting" rows={boxscore.home_batting} />
                <PlayerLines title="Away pitching" rows={boxscore.away_pitching} />
                <PlayerLines title="Home pitching" rows={boxscore.home_pitching} />
              </div>
            </>
          )}
        </div>
      )}

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
