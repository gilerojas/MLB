export default function Loading() {
  return (
    <div className="px-5 py-5 space-y-5 max-w-[1400px] animate-pulse">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-2">
          <div className="h-6 w-52 rounded bg-border" />
          <div className="h-3 w-72 rounded bg-border" />
        </div>
        <div className="h-20 w-72 rounded-lg bg-border" />
      </div>
      <div className="rounded-lg border border-border overflow-hidden">
        <div className="h-12 bg-surface-header border-b border-border" />
        <div className="p-4 grid xl:grid-cols-3 gap-4">
          <div className="xl:col-span-2 space-y-3">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="grid grid-cols-[64px_1fr] gap-3 py-3 border-b border-border/40">
                <div className="space-y-1.5">
                  <div className="h-3 w-10 rounded bg-border" />
                  <div className="h-4 w-14 rounded bg-border" />
                </div>
                <div className="space-y-1.5">
                  <div className="h-4 w-36 rounded bg-border" />
                  <div className="h-3 w-full rounded bg-border" />
                </div>
              </div>
            ))}
          </div>
          <div className="space-y-4">
            <div className="h-40 rounded-lg bg-border" />
            <div className="h-32 rounded-lg bg-border" />
          </div>
        </div>
      </div>
    </div>
  );
}
