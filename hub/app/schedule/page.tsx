export const dynamic = "force-dynamic";

interface Game {
  game_pk: number;
  away_team: string;
  home_team: string;
  away_score: number | null;
  home_score: number | null;
  status: string;
  game_date: string;
  venue: string | null;
  game_type: string;
}

async function fetchGames(): Promise<{ date: string; games: Game[] }> {
  try {
    const res = await fetch(
      `${process.env.FASTAPI_BASE_URL || "http://localhost:8000"}/schedule/today`,
      { cache: "no-store" }
    );
    if (!res.ok) throw new Error("Schedule fetch failed");
    return res.json();
  } catch {
    return { date: new Date().toISOString().slice(0, 10), games: [] };
  }
}

function statusBadge(status: string) {
  const s = status.toLowerCase();
  if (s.includes("final"))
    return (
      <span className="bg-green-100 text-green-800 text-xs px-2 py-0.5 rounded-full">
        Final
      </span>
    );
  if (s.includes("progress") || s.includes("live"))
    return (
      <span className="bg-red-100 text-red-700 text-xs px-2 py-0.5 rounded-full animate-pulse">
        Live
      </span>
    );
  return (
    <span className="bg-gray-100 text-gray-600 text-xs px-2 py-0.5 rounded-full">
      {status}
    </span>
  );
}

export default async function SchedulePage() {
  const { date, games } = await fetchGames();

  return (
    <div className="max-w-3xl mx-auto p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Today&apos;s Schedule</h1>
          <p className="text-gray-500 text-sm mt-1">{date}</p>
        </div>
        <span className="text-sm bg-blue-100 text-blue-800 px-3 py-1 rounded-full">
          {games.length} games
        </span>
      </div>

      {games.length === 0 ? (
        <div className="text-center py-16 text-gray-400">
          <p className="text-4xl mb-3">⚾</p>
          <p>No games scheduled today or schedule unavailable.</p>
          <p className="text-sm mt-2">Make sure the FastAPI server is running.</p>
        </div>
      ) : (
        <div className="space-y-3">
          {games.map((game) => (
            <div
              key={game.game_pk}
              className="bg-white rounded-lg border border-gray-200 shadow-sm p-4"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="text-center min-w-[120px]">
                    <p className="font-semibold text-gray-900">{game.away_team}</p>
                    <p className="text-gray-400 text-xs">AWAY</p>
                  </div>
                  <div className="text-center">
                    {game.away_score != null ? (
                      <p className="font-bold text-lg">
                        {game.away_score} – {game.home_score}
                      </p>
                    ) : (
                      <p className="text-gray-400 font-medium">vs</p>
                    )}
                    {statusBadge(game.status)}
                  </div>
                  <div className="text-center min-w-[120px]">
                    <p className="font-semibold text-gray-900">{game.home_team}</p>
                    <p className="text-gray-400 text-xs">HOME</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href={`http://localhost:8000/cards/batter`}
                    className="text-xs border border-[#2c7a7b] text-[#2c7a7b] hover:bg-[#2c7a7b] hover:text-white px-3 py-1.5 rounded transition-colors"
                    title="Generate batter card (use Settings or CLI)"
                  >
                    Batter Card
                  </a>
                  <a
                    href={`http://localhost:8000/cards/pitcher`}
                    className="text-xs border border-[#276749] text-[#276749] hover:bg-[#276749] hover:text-white px-3 py-1.5 rounded transition-colors"
                    title="Generate pitcher card (use Settings or CLI)"
                  >
                    Pitcher Card
                  </a>
                </div>
              </div>
              {game.venue && (
                <p className="text-xs text-gray-400 mt-2">{game.venue}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
