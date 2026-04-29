"use client";

import { useEffect, useState } from "react";

const STORAGE_KEY = "mlbops-theme";
/** Pre-mlbops rename; read once and migrate to STORAGE_KEY */
const LEGACY_THEME_KEY = "malliops-theme";

type ThemeMode = "dark" | "light";

function readStoredTheme(): ThemeMode {
  try {
    let stored = localStorage.getItem(STORAGE_KEY) as ThemeMode | null;
    if (stored !== "light" && stored !== "dark") {
      const legacy = localStorage.getItem(LEGACY_THEME_KEY) as ThemeMode | null;
      if (legacy === "light" || legacy === "dark") {
        localStorage.setItem(STORAGE_KEY, legacy);
        try {
          localStorage.removeItem(LEGACY_THEME_KEY);
        } catch {
          /* ignore */
        }
        stored = legacy;
      }
    }
    return stored === "light" || stored === "dark" ? stored : "dark";
  } catch {
    return "dark";
  }
}

function applyTheme(mode: ThemeMode) {
  document.documentElement.dataset.theme = mode;
}

export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const [mode, setMode] = useState<ThemeMode>("dark");
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const next = readStoredTheme();
    setMode(next);
    applyTheme(next);
  }, []);

  function cycle() {
    const next: ThemeMode = mode === "dark" ? "light" : "dark";
    setMode(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      /* ignore */
    }
    applyTheme(next);
  }

  if (!mounted) {
    return (
      <div
        className={`border border-border bg-surface animate-pulse ${compact ? "h-7 w-16" : "h-9 w-full"}`}
        aria-hidden
      />
    );
  }

  const title = mode === "dark" ? "Switch to light theme" : "Switch to dark theme";

  if (compact) {
    return (
      <button
        type="button"
        onClick={cycle}
        className="px-2 py-1 text-xs font-medium text-muted hover:text-foreground border border-transparent hover:border-border bg-surface-hover/50"
        title={title}
      >
        <span className="material-symbols-outlined text-base align-middle mr-1" aria-hidden>
          contrast
        </span>
        {mode === "dark" ? "Light" : "Dark"}
      </button>
    );
  }

  return (
    <button
      type="button"
      onClick={cycle}
      className="flex items-center gap-3 py-2 px-4 w-full text-left text-slate-400 hover:text-white transition-colors"
      title={title}
    >
      <span className="material-symbols-outlined text-lg shrink-0" aria-hidden>
        contrast
      </span>
      <span className="text-sm font-headline uppercase tracking-widest">Theme</span>
    </button>
  );
}
