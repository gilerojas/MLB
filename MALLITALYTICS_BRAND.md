# Mallitalytics — Brand Reference for Claude Code
# Use this file as context when generating any visualization, card, chart, or graphic.
# Last updated: April 2026

---

## 1. Identity

- **Brand:** Mallitalytics (@Mallitalytics)
- **Mission:** Data-driven baseball analytics — MLB (Apr–Oct), LIDOM (Nov–Feb), WBC
- **Voice:** Warm but analytical. Not Bloomberg cold, not ESPN loud.
- **Tagline:** *béisbol analítico sin opiniones vacías*
- **Always include** `@Mallitalytics` on every graphic (bottom-right, slate color, small)

---

## 2. Color Palette

### Primary Colors

| Token            | Hex       | RGB              | Role                                              |
|------------------|-----------|------------------|---------------------------------------------------|
| `dark_teal`      | `#2C3E50` | 44, 62, 80       | Wordmark, headers, primary text (light bg)        |
| `forest_green`   | `#2E7D32` | 46, 125, 50      | Core brand — bars, chart fills, key elements      |
| `light_green`    | `#66BB6A` | 102, 187, 106    | Secondary bars, gradients, dark mode accents      |
| `burnt_orange`   | `#E8712B` | 232, 113, 43     | THE accent — key numbers, highlights, CTAs        |
| `warm_cream`     | `#EDE8E0` | 237, 232, 224    | Light mode background                             |

### Supporting Colors

| Token            | Hex       | RGB              | Role                                              |
|------------------|-----------|------------------|---------------------------------------------------|
| `off_white`      | `#F5F2ED` | 245, 242, 237    | Alt light background, card fills                  |
| `slate`          | `#5D6D7E` | 93, 109, 126     | Labels, captions, secondary text, footer handle   |
| `charcoal`       | `#1A2530` | 26, 37, 48       | Dark mode background                              |
| `soft_red`       | `#E74C3C` | 231, 76, 60      | Negative trends, down indicators                  |
| `muted_gold`     | `#F0A830` | 240, 168, 48     | Warnings, neutral/mixed highlights                |

### Pitch Type Color Logic (family-based, not arbitrary)

| Family       | Colors (dark → light)                        | Pitch Types                        |
|--------------|----------------------------------------------|------------------------------------|
| Fastballs    | Crimson `#B71C1C` → Wine `#8B0000`           | FF, SI, FC                         |
| Offspeed     | Amber `#FF8F00` → Saddle `#8B6914`           | CH, FS, FO                         |
| Breaking     | Forest `#2E7D32` → Teal `#00796B` → Royal `#1565C0` → Indigo `#283593` | CU, SL, SV, KC |

---

## 3. Python / Matplotlib Setup

Drop this dict at the top of any visualization script:

```python
# Mallitalytics brand palette
BRAND = {
    # Primary
    "dark_teal":    "#2C3E50",
    "forest_green": "#2E7D32",
    "light_green":  "#66BB6A",
    "burnt_orange": "#E8712B",
    "warm_cream":   "#EDE8E0",
    # Supporting
    "off_white":    "#F5F2ED",
    "slate":        "#5D6D7E",
    "charcoal":     "#1A2530",
    "soft_red":     "#E74C3C",
    "muted_gold":   "#F0A830",
}

# Pitch family palettes
PITCH_COLORS = {
    "FF": "#B71C1C", "SI": "#C62828", "FC": "#8B0000",   # Fastballs: crimson family
    "CH": "#FF8F00", "FS": "#F57F17", "FO": "#8B6914",   # Offspeed: amber family
    "CU": "#2E7D32", "SL": "#00796B", "SV": "#1565C0",   # Breaking: green→teal→blue
    "KC": "#283593", "CS": "#37474F",                     # Breaking cont.
}
```

---

## 4. Card Templates

### Dark Mode Card (default for X / stat breakdowns)
```
Background:   #1A2530  (charcoal)
Primary text: #F5F2ED  (off_white)
Player name:  #F5F2ED, bold
Key numbers:  #E8712B  (burnt_orange) — the "look at this" color
Positive:     #66BB6A  (light_green)
Negative:     #E74C3C  (soft_red)
Labels:       #5D6D7E  (slate)
Bars/charts:  light_green → forest_green gradient
Footer:       "@Mallitalytics" in slate, 10–12px, bottom-right
```

### Light Mode Card (IG, storytelling posts)
```
Background:   #EDE8E0  (warm_cream)
Primary text: #2C3E50  (dark_teal)
Player name:  #2C3E50, bold
Key numbers:  #E8712B  (burnt_orange)
Positive:     #2E7D32  (forest_green)
Negative:     #E74C3C  (soft_red)
Labels:       #5D6D7E  (slate)
Bars/charts:  forest_green → light_green gradient
Footer:       "@Mallitalytics" in slate, 10–12px, bottom-right
```

---

## 5. Typography

| Context              | Font (primary)     | Fallback    | Size range  |
|----------------------|--------------------|-------------|-------------|
| Card title / name    | Inter Bold         | Arial Bold  | 28–36px     |
| Key stat number      | Inter Extra Bold   | Arial Bold  | 40–60px     |
| Body / labels        | Inter Regular      | Arial       | 14–18px     |
| Stat tables / data   | JetBrains Mono     | Courier New | 12–14px     |
| Footer / attribution | Inter Regular      | Arial       | 10–12px     |

In matplotlib, use `fontfamily='DejaVu Sans'` as the system fallback. Inter requires installation.

---

## 6. Platform Dimensions

| Platform          | Size (px)         | Notes                                       |
|-------------------|-------------------|---------------------------------------------|
| X / Twitter       | 1200 × 675        | 16:9. Keep key info center — edges crop mobile |
| IG Feed (square)  | 1080 × 1080       | 1:1                                          |
| IG Feed (portrait)| 1080 × 1350       | 4:5 — preferred for feed real estate         |
| IG Stories/TikTok | 1080 × 1920       | 9:16 — stack content vertically              |

**Default card size for Claude Code output: 1200 × 675px**

---

