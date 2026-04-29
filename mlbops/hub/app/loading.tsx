/** Shown while the root `page.tsx` RSC is resolving (e.g. `/briefing` fetch). Not the same as terminal “Compiling /”. */
export default function RootLoading() {
  return (
    <div className="px-4 py-4 space-y-4 max-w-[1320px] animate-pulse" aria-busy>
      <div className="flex flex-col gap-3 sm:flex-row sm:justify-between">
        <div className="space-y-2">
          <div className="h-6 w-36 rounded bg-surface-hover" />
          <div className="h-3 w-48 rounded bg-surface-hover/70" />
        </div>
        <div className="h-10 w-full sm:max-w-xs rounded border border-border bg-surface-hover/40" />
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <div
            key={i}
            className="h-[4.5rem] rounded-lg border border-border bg-surface-hover/50"
          />
        ))}
      </div>
      <div className="h-64 rounded-lg border border-border bg-surface-hover/35" />
    </div>
  );
}
