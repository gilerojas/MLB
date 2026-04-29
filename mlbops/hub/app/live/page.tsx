import LiveEventsClient from "@/components/LiveEventsClient";

export const dynamic = "force-dynamic";

export default function LivePage() {
  return (
    <div className="flex flex-col min-h-0 flex-1">
      <div className="px-6 py-4 border-b border-outline shrink-0 bg-background">
        <span className="text-xs font-mono text-accent-soft uppercase tracking-widest leading-none">
          Console / Live
        </span>
        <h1 className="text-2xl font-headline font-bold text-foreground mt-1">Live events</h1>
        <p className="text-xs font-mono text-slate-500 mt-1">
          In-game scanner — HR, multi-HR, no-hit bid, K milestones, cycle watch, final, debut. Scan now, then queue the ones you want to post.
        </p>
      </div>
      <LiveEventsClient />
    </div>
  );
}
