"use client";

import { Fragment, useCallback, useEffect, useRef, useState } from "react";
import { getApiBase } from "@/lib/api";

// ── types ─────────────────────────────────────────────────────────────────────

type CardType = "pitcher" | "batter";

interface PlayerResult {
  id: number;
  fullName: string;
  primaryPosition: string;
  currentTeam: string;
}

interface GameRow {
  game_pk: number | null;
  game_date: string;
  home_away: "home" | "away";
  team: string;
  opponent: string;
  stat: Record<string, unknown>;
  has_local_data: boolean;
  feed_path: string | null;
}

interface GenerateState {
  status: "idle" | "generating" | "done" | "error";
  image_url?: string;
  tweet_text?: string;
  error?: string;
  item_id?: number;
}

// ── helpers ───────────────────────────────────────────────────────────────────

function StatPill({ label, value }: { label: string; value: unknown }) {
  if (value == null || value === "") return null;
  return (
    <span className="inline-flex gap-1 text-xs">
      <span className="text-dim">{label}</span>
      <span className="font-semibold text-foreground-muted">{String(value)}</span>
    </span>
  );
}

function pitcherStats(stat: Record<string, unknown>) {
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-1">
      <StatPill label="IP" value={stat.inningsPitched} />
      <StatPill label="K" value={stat.strikeOuts} />
      <StatPill label="BB" value={stat.baseOnBalls} />
      <StatPill label="ER" value={stat.earnedRuns} />
      <StatPill label="H" value={stat.hits} />
      <StatPill label="ERA" value={stat.era} />
    </div>
  );
}

function batterStats(stat: Record<string, unknown>) {
  return (
    <div className="flex flex-wrap gap-x-3 gap-y-1">
      <StatPill label="AB" value={stat.atBats} />
      <StatPill label="H" value={stat.hits} />
      <StatPill label="HR" value={stat.homeRuns} />
      <StatPill label="RBI" value={stat.rbi} />
      <StatPill label="BB" value={stat.baseOnBalls} />
      <StatPill label="K" value={stat.strikeOuts} />
      <StatPill label="AVG" value={stat.avg} />
    </div>
  );
}

// ── main component ─────────────────────────────────────────────────────────────

export default function CardsPage() {
  const base = getApiBase();
  const [cardType, setCardType] = useState<CardType>("pitcher");
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<PlayerResult[]>([]);
  const [sugOpen, setSugOpen] = useState(false);
  const [selectedPlayer, setSelectedPlayer] = useState<PlayerResult | null>(null);
  const [games, setGames] = useState<GameRow[]>([]);
  const [gamesLoading, setGamesLoading] = useState(false);
  const [gamesError, setGamesError] = useState<string | null>(null);
  const [genStates, setGenStates] = useState<Record<string, GenerateState>>({});
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdown on outside click
  useEffect(() => {
    function handler(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setSugOpen(false);
      }
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Debounced player search
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    if (query.length < 2) {
      setSuggestions([]);
      setSugOpen(false);
      return;
    }
    debounceRef.current = setTimeout(async () => {
      try {
        const r = await fetch(`${base}/cards/players/search?q=${encodeURIComponent(query)}&limit=10`);
        if (!r.ok) return;
        const data = await r.json();
        setSuggestions(data.players || []);
        setSugOpen(true);
      } catch {
        // silent
      }
    }, 300);
  }, [query, base]);

  const selectPlayer = useCallback(
    async (p: PlayerResult) => {
      setSelectedPlayer(p);
      setQuery(p.fullName);
      setSugOpen(false);
      setSuggestions([]);
      setGames([]);
      setGamesError(null);
      setGenStates({});
      setGamesLoading(true);
      try {
        const r = await fetch(
          `${base}/cards/players/${p.id}/games?position=${cardType}&season=${new Date().getFullYear()}`
        );
        if (!r.ok) {
          const txt = await r.text();
          setGamesError(txt.slice(0, 200));
        } else {
          const data = await r.json();
          setGames(data.games || []);
        }
      } catch (e) {
        setGamesError(String(e));
      } finally {
        setGamesLoading(false);
      }
    },
    [base, cardType]
  );

  // Re-fetch games when cardType changes (if a player is already selected)
  useEffect(() => {
    if (!selectedPlayer) return;
    setGames([]);
    setGamesError(null);
    setGenStates({});
    setGamesLoading(true);
    fetch(
      `${base}/cards/players/${selectedPlayer.id}/games?position=${cardType}&season=${new Date().getFullYear()}`
    )
      .then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(t))))
      .then((data) => setGames(data.games || []))
      .catch((e) => setGamesError(String(e)))
      .finally(() => setGamesLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cardType]);

  async function generateCard(game: GameRow) {
    if (!selectedPlayer) return;
    const key = game.game_date;
    setGenStates((prev) => ({ ...prev, [key]: { status: "generating" } }));

    try {
      let endpoint: string;
      let body: Record<string, unknown>;

      if (cardType === "pitcher") {
        endpoint = `${base}/cards/pitcher`;
        body = {
          player_id: selectedPlayer.id,
          game_date: game.game_date,
        };
      } else {
        if (!game.feed_path) {
          setGenStates((prev) => ({
            ...prev,
            [key]: { status: "error", error: "No local feed file for this game." },
          }));
          return;
        }
        endpoint = `${base}/cards/batter`;
        body = {
          player_id: selectedPlayer.id,
          feed_path: game.feed_path,
        };
      }

      const r = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const data = await r.json();
      if (!r.ok) {
        setGenStates((prev) => ({
          ...prev,
          [key]: { status: "error", error: data.detail?.slice(0, 300) || "Generation failed" },
        }));
        return;
      }

      setGenStates((prev) => ({
        ...prev,
        [key]: {
          status: "done",
          image_url: data.image_url,
          tweet_text: data.tweet_text,
          item_id: data.id,
        },
      }));
    } catch (e) {
      setGenStates((prev) => ({ ...prev, [key]: { status: "error", error: String(e) } }));
    }
  }

  return (
    <div className="p-6 space-y-5 max-w-[1800px] mx-auto px-8 2xl:px-12">
      {/* Header */}
      <header>
        <h1 className="text-2xl font-headline font-bold text-foreground tracking-tight uppercase">
          Card Generator
        </h1>
        <p className="mt-1 text-xs text-muted">
          Search for a player, browse their games, and generate pitcher or batter cards.
        </p>
      </header>

      {/* Controls */}
      <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
        {/* Type toggle */}
        <div className="flex rounded-lg border border-border overflow-hidden shrink-0">
          {(["pitcher", "batter"] as CardType[]).map((t) => (
            <button
              key={t}
              type="button"
              onClick={() => setCardType(t)}
              className={`px-5 py-2 text-sm font-medium capitalize transition-colors ${
                cardType === t
                  ? "bg-info text-white"
                  : "bg-surface text-muted hover:text-foreground hover:bg-surface-hover"
              }`}
            >
              {t}
            </button>
          ))}
        </div>

        {/* Search */}
        <div className="relative flex-1 min-w-0 max-w-sm" ref={dropdownRef}>
          <input
            type="text"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              if (selectedPlayer && e.target.value !== selectedPlayer.fullName) {
                setSelectedPlayer(null);
                setGames([]);
              }
            }}
            placeholder="Search player name…"
            className="w-full rounded-lg border border-border bg-background px-4 py-2 text-sm text-foreground placeholder-dim focus:outline-none focus:border-info"
          />
          {sugOpen && suggestions.length > 0 && (
            <ul className="absolute z-40 mt-1 w-full rounded-lg border border-border bg-surface shadow-xl overflow-hidden">
              {suggestions.map((p) => (
                <li key={p.id}>
                  <button
                    type="button"
                    onMouseDown={() => selectPlayer(p)}
                    className="w-full text-left px-4 py-2.5 hover:bg-surface-hover transition-colors"
                  >
                    <span className="text-sm font-medium text-foreground">{p.fullName}</span>
                    <span className="ml-2 text-xs text-dim">
                      {p.primaryPosition} · {p.currentTeam}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        {selectedPlayer && (
          <div className="flex items-center gap-2 text-sm text-info shrink-0">
            <span className="font-semibold">{selectedPlayer.fullName}</span>
            <span className="text-dim">#{selectedPlayer.id}</span>
            <button
              type="button"
              onClick={() => {
                setSelectedPlayer(null);
                setQuery("");
                setGames([]);
                setGenStates({});
              }}
              className="text-dim hover:text-red-400 text-xs"
            >
              ✕
            </button>
          </div>
        )}
      </div>

      {/* Games table */}
      {gamesLoading && (
        <div className="space-y-2 animate-pulse">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="h-14 rounded-lg bg-surface border border-border" />
          ))}
        </div>
      )}

      {gamesError && (
        <div className="rounded-lg border border-red-900/40 bg-red-950/20 px-4 py-3 text-sm text-red-300">
          {gamesError}
        </div>
      )}

      {!gamesLoading && games.length > 0 && (
        <div className="rounded-lg border border-border overflow-hidden">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-border bg-surface-header text-muted">
                <th className="px-4 py-3 font-medium">Date</th>
                <th className="px-4 py-3 font-medium">Opponent</th>
                <th className="px-4 py-3 font-medium">Stats</th>
                <th className="px-4 py-3 font-medium">Data</th>
                <th className="px-4 py-3 font-medium w-32" />
              </tr>
            </thead>
            <tbody>
              {games.map((game) => {
                const key = game.game_date;
                const rowKey =
                  game.game_pk != null ? `${game.game_pk}-${key}` : key;
                const gs = genStates[key] ?? { status: "idle" };
                const canGenerate =
                  cardType === "pitcher" || (cardType === "batter" && game.has_local_data);
                return (
                  <Fragment key={rowKey}>
                    <tr className="border-b border-border/60 last:border-0 hover:bg-surface-hover/60 align-middle">
                      <td className="px-4 py-3 font-mono text-info text-xs whitespace-nowrap">
                        {game.game_date}
                        <span className="ml-1.5 text-dim uppercase text-xs">
                          {game.home_away}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-foreground-muted whitespace-nowrap">
                        {game.opponent || "—"}
                      </td>
                      <td className="px-4 py-3">
                        {cardType === "pitcher"
                          ? pitcherStats(game.stat)
                          : batterStats(game.stat)}
                      </td>
                      <td className="px-4 py-3">
                        {game.has_local_data ? (
                          <span className="inline-block rounded border border-emerald-800/40 bg-emerald-950/30 px-2 py-0.5 text-xs font-semibold text-emerald-400">
                            local
                          </span>
                        ) : cardType === "batter" ? (
                          <span className="inline-block rounded border border-border bg-surface-hover px-2 py-0.5 text-xs text-dim">
                            no file
                          </span>
                        ) : (
                          <span className="inline-block rounded border border-border bg-surface-hover px-2 py-0.5 text-xs text-dim">
                            api
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-right">
                        {gs.status === "idle" && (
                          <button
                            type="button"
                            onClick={() => generateCard(game)}
                            disabled={!canGenerate}
                            className={`rounded px-3 py-1.5 text-xs font-medium transition-colors ${
                              canGenerate
                                ? "bg-info text-white hover:opacity-90"
                                : "bg-surface-hover text-dim cursor-not-allowed"
                            }`}
                          >
                            Generate
                          </button>
                        )}
                        {gs.status === "generating" && (
                          <span className="text-xs text-muted animate-pulse">Generating…</span>
                        )}
                        {gs.status === "done" && (
                          <span className="text-xs text-emerald-400 font-medium">Done ✓</span>
                        )}
                        {gs.status === "error" && (
                          <button
                            type="button"
                            onClick={() => generateCard(game)}
                            className="text-xs text-red-400 hover:underline"
                            title={gs.error}
                          >
                            Retry
                          </button>
                        )}
                      </td>
                    </tr>
                    {/* Inline card preview */}
                    {gs.status === "done" && gs.image_url && (
                      <tr className="border-b border-border/40 bg-background">
                        <td colSpan={5} className="px-4 py-4">
                          <div className="flex flex-col lg:flex-row gap-4 items-start">
                            <a href={gs.image_url} target="_blank" rel="noreferrer">
                              {/* eslint-disable-next-line @next/next/no-img-element */}
                              <img
                                src={gs.image_url}
                                alt="Generated card"
                                className="rounded-lg border border-border max-w-full lg:max-w-2xl h-auto hover:opacity-90 transition-opacity"
                              />
                            </a>
                            <div className="space-y-2 min-w-0">
                              {gs.tweet_text && (
                                <div className="rounded-lg border border-border bg-surface p-3 max-w-sm">
                                  <p className="text-xs font-semibold uppercase tracking-wider text-dim mb-1.5">
                                    Tweet text
                                  </p>
                                  <p className="text-sm text-foreground-muted whitespace-pre-wrap leading-relaxed">
                                    {gs.tweet_text}
                                  </p>
                                </div>
                              )}
                              {gs.item_id && (
                                <p className="text-xs text-dim">
                                  Added to queue as item{" "}
                                  <span className="font-mono text-info">#{gs.item_id}</span>
                                </p>
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                    {gs.status === "error" && (
                      <tr className="border-b border-border/40 bg-red-950/10">
                        <td colSpan={5} className="px-4 py-2 text-xs text-red-300">
                          {gs.error}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {!gamesLoading && !gamesError && selectedPlayer && games.length === 0 && (
        <p className="text-sm text-dim">No games found for this season.</p>
      )}

      {!selectedPlayer && !gamesLoading && (
        <div className="rounded-lg border border-border bg-surface p-6 text-sm text-muted space-y-1">
          <p className="font-semibold text-foreground text-base">Get started</p>
          <p>Select <strong className="text-foreground-muted">Pitcher</strong> or <strong className="text-foreground-muted">Batter</strong>, then search for a player by name.</p>
          <p className="text-xs text-dim">
            Pitcher cards use the MLB Stats API (no local file needed). Batter cards require a local
            feed file — rows marked <span className="font-mono text-emerald-400">local</span> have
            one available.
          </p>
        </div>
      )}
    </div>
  );
}
