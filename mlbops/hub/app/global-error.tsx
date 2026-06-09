"use client";

export default function GlobalError({
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html lang="en">
      <body>
        <main className="min-h-screen bg-background text-foreground p-8">
          <h1 className="text-xl font-bold">Mallitalytics Hub</h1>
          <p className="mt-2 text-sm text-muted">The Hub hit a rendering error.</p>
          <button
            type="button"
            onClick={() => reset()}
            className="mt-4 border border-outline px-3 py-2 text-sm"
          >
            Retry
          </button>
        </main>
      </body>
    </html>
  );
}
