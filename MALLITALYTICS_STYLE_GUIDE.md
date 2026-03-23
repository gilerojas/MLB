# Mallitalytics — Brand Style Guide

A loose reference for visual consistency across X, IG, TikTok, and generated graphics.
Not a rigid corporate guide — just enough to look cohesive without overthinking it.

Derived from the official Mallitalytics logo.

---

## Color Palette

### Primary Colors (from logo)

| Name | Hex | RGB | Usage |
|------|-----|-----|-------|
| **Dark Teal** | `#2C3E50` | 44, 62, 80 | Wordmark, headers, primary text |
| **Forest Green** | `#2E7D32` | 46, 125, 50 | Core brand color — bars, charts, key elements |
| **Light Green** | `#66BB6A` | 102, 187, 106 | Secondary charts, lighter bars, gradients |
| **Burnt Orange** | `#E8712B` | 232, 113, 43 | Primary accent — highlights, key numbers, CTAs |
| **Warm Cream** | `#EDE8E0` | 237, 232, 224 | Light backgrounds |

### Supporting Colors

| Name | Hex | RGB | Usage |
|------|-----|-----|-------|
| **Off White** | `#F5F2ED` | 245, 242, 237 | Alt light background, card fills |
| **Slate** | `#5D6D7E` | 93, 109, 126 | Secondary text, labels, captions |
| **Charcoal** | `#1A2530` | 26, 37, 48 | Dark mode background |
| **Soft Red** | `#E74C3C` | 231, 76, 60 | Negative trends, down indicators |
| **Muted Gold** | `#F0A830` | 240, 168, 48 | Warnings, neutral highlights |

---

## Dark Mode vs Light Mode

The logo has a warm, light-background feel. Content can go either way:

### Light Mode (matches logo vibe)
- Background: Warm Cream (`#EDE8E0`) or Off White (`#F5F2ED`)
- Text: Dark Teal (`#2C3E50`)
- Accents: Forest Green + Burnt Orange
- Best for: IG, cleaner graphics, storytelling posts

### Dark Mode (analytics/terminal feel)
- Background: Charcoal (`#1A2530`)
- Text: Off White (`#F5F2ED`)
- Accents: Light Green (`#66BB6A`) + Burnt Orange (`#E8712B`)
- Best for: X pitcher cards, stat breakdowns, data-heavy graphics

Both modes use the same green + orange accent system, so they feel connected.

---

## Typography

### Recommended Fonts

| Context | Font | Fallback | Notes |
|---------|------|----------|-------|
| **Headlines / Player Names** | **Inter Bold** or **Roboto Bold** | Arial Bold | Clean, modern, matches logo's sans-serif feel |
| **Body text** | **Inter Regular** | Arial | Pairs with bold headlines |
| **Data / Stats** | **JetBrains Mono** or **Fira Code** | Courier New | Monospace for stat tables |

### Font Sizing (for graphics)

- Card title / player name: 28-36px
- Key stat number: 40-60px (use Burnt Orange or Forest Green to make these pop)
- Labels and secondary text: 14-18px
- Fine print / source attribution: 10-12px

---

## Visual Rules

### Pitcher Card / Batter Card
- Background: Charcoal (`#1A2530`) or Warm Cream (`#EDE8E0`)
- Player name: Dark Teal (light bg) or Off White (dark bg)
- Big stat numbers: Burnt Orange (`#E8712B`) — this is your "look at this" color
- Positive trends: Forest Green (`#2E7D32`)
- Negative trends: Soft Red (`#E74C3C`)
- Stat labels: Slate (`#5D6D7E`)
- Chart bars / visual elements: Green gradient (Light Green → Forest Green)
- Footer: "@Mallitalytics" in Slate, small

### HR Tracker / Game Review
- HR distances / EV numbers: Burnt Orange for emphasis
- Player names: Dark Teal or Off White depending on bg
- Pitch type labels: Slate italic

### Weekly Recap Thread
- Mix light and dark cards for visual variety across the thread
- First tweet card: dark mode (grabs attention in feed)
- Interior tweets: can alternate

---

## Logo Usage

- On light backgrounds: Use logo as-is
- On dark backgrounds: Invert the cream to dark, keep green bars and orange dot
- Always include `@Mallitalytics` on graphics, bottom-right corner
- Logo should be subtle — the content is the star

---

## The Brand Feel

The logo tells the story: **growth charts + data + a sharp accent point.** The visual identity should feel like:

- **Warm but analytical** — not cold Bloomberg terminal, not flashy ESPN
- **Green = growth, depth, baseball grass** — it's the core identity color
- **Orange = the insight, the number that matters** — use it sparingly for maximum impact
- Think: a well-designed analytics dashboard that happens to love baseball

---

## Quick Reference — Copy/Paste Hex Codes

```
Primary (from logo):
  Dark Teal:      #2C3E50
  Forest Green:   #2E7D32
  Light Green:    #66BB6A
  Burnt Orange:   #E8712B
  Warm Cream:     #EDE8E0

Supporting:
  Off White:      #F5F2ED
  Slate:          #5D6D7E
  Charcoal:       #1A2530
  Soft Red:       #E74C3C
  Muted Gold:     #F0A830
```

### Python / Matplotlib Quick Setup
```python
MALLITALYTICS = {
    "dark_teal": "#2C3E50",
    "forest_green": "#2E7D32",
    "light_green": "#66BB6A",
    "burnt_orange": "#E8712B",
    "warm_cream": "#EDE8E0",
    "off_white": "#F5F2ED",
    "slate": "#5D6D7E",
    "charcoal": "#1A2530",
    "soft_red": "#E74C3C",
    "muted_gold": "#F0A830",
}
```

---

## Platform Notes

- **X (Twitter):** 1200x675px (16:9). Keep key info center — edges crop on mobile.
- **IG Feed:** 1080x1080 (1:1) or 1080x1350 (4:5). Reformat X cards vertically.
- **IG Stories / TikTok:** 1080x1920 (9:16). Stack card content vertically.

---

*Last updated: Feb 2026*
