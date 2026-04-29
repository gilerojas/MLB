export default function Loading() {
  return (
    <div className="px-5 py-5 space-y-5 max-w-[1400px] animate-pulse">
      <div className="space-y-1.5">
        <div className="h-6 w-40 rounded bg-border" />
        <div className="h-3 w-80 rounded bg-border" />
      </div>
      <div className="flex gap-3 items-center">
        <div className="h-9 w-36 rounded-lg bg-border" />
        <div className="h-9 w-64 rounded-lg bg-border" />
      </div>
      <div className="rounded-lg border border-border overflow-hidden">
        <div className="h-10 bg-surface-header border-b border-border" />
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="h-14 border-b border-border/50 bg-surface/30" />
        ))}
      </div>
    </div>
  );
}
