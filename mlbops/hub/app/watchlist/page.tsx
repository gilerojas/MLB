import WatchlistClient from "@/components/WatchlistClient";

export const dynamic = "force-dynamic";

export default function WatchlistPage() {
  return (
    <div className="p-6 max-w-[1800px] mx-auto px-8 2xl:px-12">
      <div className="mb-4">
        <h1 className="text-2xl font-headline font-bold text-foreground uppercase tracking-tight">
          Player watchlist
        </h1>
        <p className="text-xs text-muted mt-1">
          Writes <span className="font-mono">jobs/player_watchlist.json</span> and syncs{" "}
          <span className="font-mono">player_watchlist</span> in hub.db. Next morning_intel run reads the JSON.
        </p>
      </div>
      <WatchlistClient />
    </div>
  );
}
