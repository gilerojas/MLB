# Mallitalytics Brand System

Single source of truth for Mallitalytics visuals across the MLB Ops hub, X/IG graphics, cards, charts, and generated UI.

Last updated: June 2026

---

## 1. Identity

**Brand:** Mallitalytics (`@Mallitalytics`)

**Mission:** Data-driven baseball analytics for MLB, LIDOM, and international baseball.

**Voice:** Warm but analytical. Not Bloomberg cold, not ESPN loud.

**Tagline:** `beisbol analitico sin opiniones vacias`

**Design principle:** Data is the hero. Decoration must either guide attention, explain the number, or reinforce the baseball context.

Always include `@Mallitalytics` on graphics, usually bottom-right in slate or cream depending on background.

---

## 2. Color System

Use the logo-derived palette as the source of truth. Do not introduce extra colors unless the metric requires a distinct semantic state.

```css
:root {
  --malli-charcoal:    #1A2530;  /* dark mode base */
  --malli-dark:        #2C3E50;  /* wordmark, headers, primary text on light */
  --malli-green:       #2E7D32;  /* core brand, bars, positive data */
  --malli-light-green: #66BB6A;  /* secondary green, positive accents */
  --malli-sage:        #A5B884;  /* secondary data highlight */
  --malli-orange:      #E8712B;  /* primary insight / CTA / key number */
  --malli-cream:       #EDE8E0;  /* warm light background */
  --malli-off-white:   #F5F2ED;  /* text on dark, card fill on light */
  --malli-slate:       #5D6D7E;  /* labels, captions, footer */
  --malli-red:         #E74C3C;  /* negative trend, damage, warning */
  --malli-gold:        #F0A830;  /* neutral/mixed highlight */
}
```

### Usage

- `--malli-orange` is the "look here" color. Use it sparingly for the number or action that matters most.
- `--malli-green` and `--malli-light-green` carry positive signals, chart fills, and baseball-growth identity.
- `--malli-sage` is for secondary highlights when green/orange would compete with the main insight.
- `--malli-slate` is for metadata, labels, captions, handles, and non-primary controls.
- Avoid one-note screens: do not make everything green, everything charcoal, or everything orange.

### Dark Mode

```css
background: #1A2530;
surface:    #243342;
text:       #F5F2ED;
labels:     #5D6D7E;
accent:     #E8712B;
positive:   #66BB6A;
```

Best for X, pitching cards, stat breakdowns, data-heavy dashboards, and queue/intel workflows.

### Light Mode

```css
background: #EDE8E0;
surface:    #F5F2ED;
text:       #2C3E50;
labels:     #5D6D7E;
accent:     #E8712B;
positive:   #2E7D32;
```

Best for IG, storytelling graphics, recap cards, and more editorial posts.

---

## 3. Typography

Montserrat is the brand font.

```css
--font-brand: "Montserrat";
--font-data: "JetBrains Mono";
```

### Rules

- Headlines / player names: `Montserrat SemiBold` or `Montserrat Bold`
- Body / labels: `Montserrat Regular` or `Montserrat Medium`
- Stats / tables: `JetBrains Mono` only where alignment matters
- Avoid Inter, Roboto, Arial, and Space Grotesk as primary fonts
- Do not scale font size with viewport width
- Letter spacing should be `0` for normal text and at most `0.1em` for labels

### Type Scale

```css
--text-xs:   0.75rem;   /* 12px */
--text-sm:   0.875rem;  /* 14px */
--text-base: 1rem;      /* 16px */
--text-lg:   1.125rem;  /* 18px */
--text-xl:   1.25rem;   /* 20px */
--text-2xl:  1.5rem;    /* 24px */
--text-3xl:  1.875rem;  /* 30px */
--text-4xl:  2.25rem;   /* 36px */
--text-5xl:  3rem;      /* 48px */
```

---

## 4. Layout

Use an 8px base grid.

```css
--space-1:  4px;
--space-2:  8px;
--space-3:  12px;
--space-4:  16px;
--space-6:  24px;
--space-8:  32px;
--space-10: 40px;
--space-12: 48px;
--space-16: 64px;
--space-20: 80px;
--space-24: 96px;
```

### Rules

- Border radius: `6px`
- Data tables may use borders; cards and panels should use restrained shadows or surface contrast
- Keep card internals symmetrical: equal top/left rhythm unless the data format requires otherwise
- Buttons and controls must not wrap text on mobile
- Fixed-format UI elements need stable dimensions with explicit constraints
- Do not put cards inside cards
- Operational screens should prioritize dense, scannable information over marketing-style hero layouts

---

## 5. Icons

Preferred icon library: Phosphor Icons.

Avoid using Lucide as the default for new UI. Existing Lucide usage can remain until the component is being redesigned.

Use icons for tools and repeated controls where the symbol is familiar. Pair with tooltips when the meaning is not obvious.

---

## 6. Motion

Use one intentional motion pattern per screen.

```css
.reveal {
  opacity: 0;
  transform: translateY(16px);
  animation: fadeUp 0.4s ease forwards;
}

.reveal:nth-child(1) { animation-delay: 0s; }
.reveal:nth-child(2) { animation-delay: 0.08s; }
.reveal:nth-child(3) { animation-delay: 0.16s; }

@keyframes fadeUp {
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
```

### Rules

- UI motion: `200ms` to `400ms`
- Hero/large reveal motion: `600ms` to `800ms`
- Easing: `ease`, `ease-out`, or `cubic-bezier(0.16, 1, 0.3, 1)`
- Do not use scattered hover animations or slow `1s+` transitions

---

## 7. Graphics

### Platform Sizes

| Platform | Size | Notes |
|---|---:|---|
| X / Twitter | `1200 x 675` | Default. Keep key info centered for mobile crop. |
| IG Feed Square | `1080 x 1080` | Useful for rankings and leaderboard cards. |
| IG Feed Portrait | `1080 x 1350` | Preferred for feed real estate. |
| Stories / TikTok | `1080 x 1920` | Stack content vertically. |

### Card Defaults

- Dark stat card background: `#1A2530`
- Light editorial card background: `#EDE8E0`
- Key stat number: `#E8712B`
- Positive metric: `#66BB6A` or `#2E7D32`
- Negative metric: `#E74C3C`
- Labels and footer: `#5D6D7E`
- Footer: `@Mallitalytics`, small, bottom-right

### Pitch Type Colors

Pitch type colors should be family-based, not arbitrary.

```python
PITCH_COLORS = {
    "FF": "#B71C1C", "SI": "#C62828", "FC": "#8B0000",
    "CH": "#FF8F00", "FS": "#F57F17", "FO": "#8B6914",
    "CU": "#2E7D32", "SL": "#00796B", "SV": "#1565C0",
    "KC": "#283593", "CS": "#37474F",
}
```

---

## 8. Anti-Slop Rules

Never ship these:

- Random glow blobs or ambient lights scattered across the layout
- Purple gradients
- Generic stock hero images
- Decorative badges with no information value
- Six or more active colors fighting on one screen
- Inter, Roboto, Arial, Space Grotesk, or system fonts as the primary brand font
- Lucide icons as the default for new design work
- Button text wrapping into two lines
- Inconsistent spacing between related sections
- Active states that are only a thin border with no real affordance

### Fix Patterns

- Use one dominant color, one accent, and one neutral per screen
- Use a single anchored glow only if it explains focus or hierarchy
- Use baseball-specific visuals: pitch plots, spray charts, field geometry, leaderboards, real/generated photography-style assets
- Use background texture or subtle surface contrast instead of decoration
- Make active states use background fill, color, and weight together

---

## 9. Prompt Prefix

Use this for Mallitalytics UI or graphics generation:

```text
Brand: Mallitalytics. Use Montserrat as the primary brand font and JetBrains Mono for aligned stats. Palette: #1A2530, #2C3E50, #2E7D32, #66BB6A, #A5B884, #E8712B, #EDE8E0, #F5F2ED, #5D6D7E. Border radius: 6px. Data is the hero. No purple gradients, no random glows, no generic stock visuals, no decorative status badges. Use an 8px spacing grid and one intentional motion pattern only.
```

---

## 10. Pre-Ship Checklist

- [ ] Uses only Mallitalytics palette colors, unless a metric requires a semantic exception
- [ ] Montserrat is the primary font
- [ ] JetBrains Mono is used only for aligned stat/data contexts
- [ ] Spacing follows the 8px grid
- [ ] Border radius is consistent at `6px`
- [ ] Orange highlights only the primary insight/action
- [ ] Active states are visually clear
- [ ] No text wraps inside buttons or compact controls
- [ ] No random glows, purple gradients, generic badges, or stock-looking filler
- [ ] `@Mallitalytics` appears on graphics

