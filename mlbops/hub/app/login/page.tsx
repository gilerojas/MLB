"use client";

import { FormEvent, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

export default function LoginPage() {
  const router = useRouter();
  const search = useSearchParams();
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(typeof data.error === "string" ? data.error : "Login failed.");
        return;
      }
      router.replace(search.get("next") || "/queue");
      router.refresh();
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center px-4 bg-background">
      <form
        onSubmit={submit}
        className="w-full max-w-sm border border-outline-variant/40 bg-surface p-5 space-y-4"
      >
        <div>
          <p className="text-xs font-mono uppercase tracking-widest text-accent">Private access</p>
          <h1 className="text-2xl font-headline font-bold text-foreground mt-1">Mallitalytics Hub</h1>
          <p className="text-sm text-dim mt-1">Sign in before posting to X.</p>
        </div>
        <label className="block">
          <span className="text-xs font-mono uppercase text-muted">Password</span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            autoFocus
            className="mt-1 w-full border border-outline-variant bg-surface-lowest px-3 py-3 text-base text-foreground outline-none focus:border-accent"
          />
        </label>
        {error && <p className="text-sm text-danger">{error}</p>}
        <button
          type="submit"
          disabled={busy || !password}
          className="w-full py-3 bg-accent text-[#552000] font-headline font-bold uppercase tracking-widest text-xs disabled:opacity-40"
        >
          {busy ? "Checking..." : "Unlock"}
        </button>
      </form>
    </main>
  );
}

