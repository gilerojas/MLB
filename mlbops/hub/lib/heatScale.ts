/** Cold (low) → warm (high) scale for highlighted stats. */

const COLD = { r: 99, g: 166, b: 232 }; // cool blue
const WARM = { r: 240, g: 168, b: 48 }; // muted gold
const PEAK = { r: 232, g: 113, b: 43 }; // burnt orange

function clamp01(t: number): number {
  return Math.max(0, Math.min(1, t));
}

function smoothstep(t: number): number {
  const x = clamp01(t);
  return x * x * (3 - 2 * x);
}

function lerpRgb(
  a: { r: number; g: number; b: number },
  b: { r: number; g: number; b: number },
  t: number,
): string {
  const s = smoothstep(t);
  const r = Math.round(a.r + (b.r - a.r) * s);
  const g = Math.round(a.g + (b.g - a.g) * s);
  const bl = Math.round(a.b + (b.b - a.b) * s);
  return `rgb(${r}, ${g}, ${bl})`;
}

export function heatColor(value: number | null | undefined, lo: number, hi: number): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "var(--muted)";
  const span = hi - lo;
  const t = span <= 0 ? 0.5 : clamp01((value - lo) / span);
  if (t < 0.92) return lerpRgb(COLD, WARM, t);
  const u = (t - 0.92) / 0.08;
  return lerpRgb(WARM, PEAK, u);
}

export const PROJ_RANGES = {
  malli: { lo: 35, hi: 75 },
  ip: { lo: 4, hi: 7 },
  k: { lo: 3, hi: 10 },
  bb: { lo: 0.5, hi: 3.5 },
  h: { lo: 3, hi: 8 },
  er: { lo: 0.5, hi: 5 },
  whip: { lo: 0.9, hi: 1.8 },
} as const;
