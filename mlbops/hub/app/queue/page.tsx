import QueueClient from "@/components/QueueClient";

export const dynamic = "force-dynamic";

export default function QueuePage() {
  return (
    <div className="flex flex-col min-h-0 flex-1">
      <div className="px-6 py-4 border-b border-outline shrink-0 bg-background">
        <span className="text-xs font-mono text-accent-soft uppercase tracking-widest leading-none">
          Console / Queue
        </span>
        <h1 className="text-2xl font-headline font-bold text-foreground mt-1">Launch station</h1>
        <p className="text-xs font-mono text-slate-500 mt-1">Generate, review, and post to X</p>
      </div>
      <QueueClient />
    </div>
  );
}
