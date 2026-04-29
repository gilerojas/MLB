/** Mallitalytics bar-chart + trend mark — fixed brand colors (readable on light + dark shells). */
export function MalliBrandMark({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 64 64"
      fill="none"
      width={26}
      height={26}
      aria-hidden
    >
      <rect x="8" y="38" width="9" height="18" fill="#2E7D32" />
      <rect x="20" y="31" width="9" height="25" fill="#2E7D32" />
      <rect x="32" y="24" width="9" height="32" fill="#2E7D32" />
      <rect x="44" y="17" width="9" height="39" fill="#2E7D32" />
      <polyline
        points="9,30 20,22 26,26 34,18 44,22 50,10"
        stroke="#2C3E50"
        strokeWidth="2.8"
        strokeLinecap="round"
        strokeLinejoin="round"
        fill="none"
      />
      <circle cx="50" cy="10" r="5.2" fill="#E8712B" />
    </svg>
  );
}
