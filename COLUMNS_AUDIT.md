# Auditoría de Columnas - Pitches Enriched Schema

## Metodología de Selección

**Criterios para INCLUIR:**
1. ✅ Esencial para análisis de pitching (velocidad, spin, movimiento)
2. ✅ Contexto del juego (score, runners, outs)
3. ✅ Resultados del pitch (descripción, evento, tipo)
4. ✅ Calidad de contacto (EV, LA, xwOBA)
5. ✅ Identificadores críticos (game_pk, pitcher_id, batter_id)

**Criterios para EXCLUIR:**
1. ❌ Deprecated (spin_rate_deprecated, break_angle_deprecated)
2. ❌ Siempre NULL (umpire, sv_id, tfs_deprecated)
3. ❌ Posiciones de fielders (fielder_2 a fielder_9) - no útil para pitching
4. ❌ Vectores de física raw (vx0, vy0, vz0, ax, ay, az) - redundante con otras métricas
5. ❌ Métricas legacy duplicadas (age_pit_legacy, age_bat_legacy)

---

## Categorización de las 118 Columnas

### 🟢 CORE - Identificadores & Contexto (KEEP: 16)

| # | Columna | Origen | Justificación |
|---|---------|--------|---------------|
| — | play_id | **FEED** | UUID del pitch en feed/live — trazabilidad al raw |
| 69 | game_pk | Statcast | ID único del juego |
| 67 | game_date | Statcast | Fecha del juego |
| 68 | game_type | Statcast | R/S/P/etc |
| 103 | inning | Statcast | Entrada del juego |
| 104 | inning_topbot | Statcast | Top/Bot |
| 80 | pitcher | Statcast | ID del pitcher |
| 79 | batter | Statcast | ID del batter |
| 78 | player_name | Statcast | Nombre del pitcher |
| 97 | stand | Statcast | L/R del batter |
| 98 | p_throws | Statcast | L/R del pitcher |
| 0 | home_team | Statcast | Equipo local |
| 1 | away_team | Statcast | Equipo visitante |
| 105 | at_bat_number | Statcast | # de turno |
| 106 | pitch_number | Statcast | # de pitch en el PA |
| 55 | n_thruorder_pitcher | Statcast | Veces que pitcher ve lineup |

---

### 🟢 PITCH CHARACTERISTICS - Stuff (KEEP: 14)

| # | Columna | Justificación |
|---|---------|---------------|
| 70 | release_speed | Velocidad del pitch ⭐ |
| 75 | release_spin_rate | RPM ⭐ |
| 94 | pitch_type | FF/SL/CH/etc ⭐ |
| 30 | pitch_name | "4-Seam Fastball" |
| 76 | release_extension | Punto de release |
| 71 | release_pos_x | Posición X release |
| 72 | release_pos_z | Posición Z release |
| 77 | release_pos_y | Posición Y release |
| 73 | plate_x | Ubicación X en el plate |
| 74 | plate_z | Ubicación Z en el plate |
| 2 | pfx_x | Movimiento horizontal ⭐ |
| 3 | pfx_z | Movimiento vertical ⭐ |
| 111 | spin_axis | Eje del spin |
| 60 | arm_angle | Ángulo del brazo |

---

### 🟢 PITCH OUTCOMES - Results (KEEP: 8)

| # | Columna | Justificación |
|---|---------|---------------|
| 95 | description | "swinging_strike", "ball", etc ⭐ |
| 115 | events | "strikeout", "home_run", etc ⭐ |
| 99 | type | S/B/X |
| 96 | zone | Zona del strike zone (1-14) |
| 101 | balls | Count de bolas |
| 102 | strikes | Count de strikes |
| 116 | des | Descripción del play completo |
| 100 | bb_type | "fly_ball", "ground_ball" |

---

### 🟢 BATTED BALL - Contact Quality (KEEP: 12)

| # | Columna | Justificación |
|---|---------|---------------|
| 86 | launch_speed | Exit velocity ⭐ |
| 87 | launch_angle | Launch angle ⭐ |
| 88 | launch_speed_angle | Categoría Savant (1–6), barrel = 2 ⭐ |
| 85 | hit_distance_sc | Distancia proyectada |
| 82 | hit_location | Zona del campo (1-9) |
| 83 | hc_x | Coordenada X del hit |
| 84 | hc_y | Coordenada Y del hit |
| 90 | estimated_woba_using_speedangle | xwOBA ⭐ |
| 89 | estimated_ba_using_speedangle | xBA |
| 93 | estimated_slg_using_speedangle | xSLG (power) |
| 91 | woba_value | wOBA real |
| 92 | woba_denom | Denominador para wOBA |

---

### 🟢 GAME STATE - Context (KEEP: 11)

| # | Columna | Justificación |
|---|---------|---------------|
| 31 | home_score | Score del home |
| 32 | away_score | Score del away |
| 47 | home_score_diff | Diferencial |
| 7 | outs_when_up | Outs en el PA |
| 4 | on_3b | Runner en 3ra |
| 5 | on_2b | Runner en 2da |
| 6 | on_1b | Runner en 1ra |
| 39 | if_fielding_alignment | Alineación IF |
| 40 | of_fielding_alignment | Alineación OF |
| 53 | age_pit | Edad del pitcher |
| 54 | age_bat | Edad del batter |

---

### 🟢 ADVANCED METRICS - Win Probability & Expected (KEEP: 6)

| # | Columna | Justificación |
|---|---------|---------------|
| 41 | delta_home_win_exp | Cambio en WE |
| 42 | delta_run_exp | Cambio en RE |
| 45 | delta_pitcher_run_exp | RE para pitcher |
| 49 | home_win_exp | WE del home |
| 50 | bat_win_exp | WE del batter |
| 28 | babip_value | BABIP value |

---

### 🟡 OPTIONAL - Advanced Physics (KEEP: 6)

| # | Columna | Justificación |
|---|---------|---------------|
| 19 | effective_speed | Velocidad efectiva |
| 17 | sz_top | Top del SZ |
| 18 | sz_bot | Bottom del SZ |
| 112 | api_break_z_with_gravity | Break vertical con gravedad |
| 113 | api_break_x_arm | Break desde brazo |
| 114 | api_break_x_batter_in | Break hacia batter |

---

### 🟡 OPTIONAL - Bat Tracking (KEEP si disponible: 5)

| # | Columna | Justificación |
|---|---------|---------------|
| 43 | bat_speed | Velocidad del bat |
| 44 | swing_length | Longitud del swing |
| 61 | attack_angle | Ángulo de ataque |
| 62 | attack_direction | Dirección del ataque |
| 46 | hyper_speed | Velocidad en swings |

---

### 🔴 EXCLUDE - Deprecated & Always NULL (DROP: 11)

| # | Columna | Razón |
|---|---------|-------|
| 8 | tfs_deprecated | Deprecated |
| 9 | tfs_zulu_deprecated | Deprecated |
| 10 | umpire | Siempre NULL (umpire viene del boxscore) |
| 107 | spin_dir | Deprecated |
| 108 | spin_rate_deprecated | Deprecated |
| 109 | break_angle_deprecated | Deprecated |
| 110 | break_length_deprecated | Deprecated |
| 117 | sv_id | Siempre NULL |
| 51 | age_pit_legacy | Duplicado (usa age_pit) |
| 52 | age_bat_legacy | Duplicado (usa age_bat) |
| 29 | iso_value | Calculable si necesitas |

---

### 🔴 EXCLUDE - Fielder Positions (DROP: 8)

No son útiles para análisis de pitching. Si necesitas defensive positioning, usa alignment fields.

| # | Columna | Razón |
|---|---------|-------|
| 20-27 | fielder_2 a fielder_9 | No relevante para pitching |

---

### 🔴 EXCLUDE - Raw Physics Vectors (DROP: 6)

Redundante con release_speed, pfx_x, pfx_z. Solo útil para reconstrucción física avanzada.

| # | Columna | Razón |
|---|---------|-------|
| 11 | vx0 | Redundante |
| 12 | vy0 | Redundante |
| 13 | vz0 | Redundante |
| 14 | ax | Redundante |
| 15 | ay | Redundante |
| 16 | az | Redundante |

---

### 🔴 EXCLUDE - Post-Score Fields (DROP: 4)

Ya tienes score actual, no necesitas score después del pitch.

| # | Columna | Razón |
|---|---------|-------|
| 35 | post_away_score | Redundante |
| 36 | post_home_score | Redundante |
| 37 | post_bat_score | Redundante |
| 38 | post_fld_score | Redundante |

---

### 🔴 EXCLUDE - Duplicate Context (DROP: 2)

| # | Columna | Razón |
|---|---------|-------|
| 33 | bat_score | Duplicado (calculable de home/away) |
| 34 | fld_score | Duplicado |

---

### 🔴 EXCLUDE - Rest Days (DROP: 4)

Interesante pero no crítico para pitch-level. Si lo necesitas, agrégalo a pitcher-level aggregations.

| # | Columna | Razón |
|---|---------|-------|
| 56 | pitcher_days_since_prev_game | No crítico |
| 57 | batter_days_since_prev_game | No crítico |
| 58 | pitcher_days_until_next_game | No crítico |
| 59 | batter_days_until_next_game | No crítico |

---

### 🔴 EXCLUDE - Swing Path Advanced (DROP: 3)

Muy niche, solo útil para análisis de swing mecánico profundo.

| # | Columna | Razón |
|---|---------|-------|
| 63 | swing_path_tilt | Muy específico |
| 64 | intercept_ball_minus_batte... | Muy específico |
| 65 | intercept_ball_minus_batte... | Muy específico |

---


---

### 🔴 EXCLUDE - Prior PA Count (DROP: 1)

| # | Columna | Razón |
|---|---------|-------|
| 81 | n_priorpa_thisgame_player_at_bat | No muy útil |

---

---

## Arquitectura del Join: Feed vs Statcast

**¿De dónde sale cada columna de `pitches_enriched`?**

### Base = Statcast (pybaseball)

Statcast tiene **casi todo**. Las 118 columnas vienen de Baseball Savant vía `statcast_single_game(game_pk)`. Del audit seleccionamos ~80.

### Única columna desde Feed/Live

| Columna | Origen | Cómo se obtiene |
|---------|--------|-----------------|
| **play_id** | feed/live | `playEvents[].playId` — UUID del evento pitch |

**Por qué:** Statcast tiene `sv_id` pero está siempre NULL. El feed/live sí incluye `playId` por cada pitch. Útil para:
- Trazabilidad al raw JSON
- Debug
- Join futuro con otra fuente

### Lógica del join

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  pitches_enriched = Statcast + play_id (del feed)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. df_statcast = statcast_single_game(game_pk)                              │
│     → 118 columnas                                                           │
│                                                                             │
│  2. df_statcast = df_statcast[COLUMNS_TO_KEEP]                               │
│     → ~80 columnas seleccionadas                                             │
│                                                                             │
│  3. df_feed_ids = extraer del raw JSON:                                      │
│       for play in allPlays:                                                  │
│         for ev in play["playEvents"]:                                        │
│           if ev.get("isPitch") and "pitchData" in ev:                        │
│             row = {game_pk, inning, at_bat_index, pitch_number, play_id}     │
│                                                                             │
│  4. merged = df_statcast.merge(                                              │
│         df_feed_ids,                                                         │
│         on=["game_pk", "inning", "at_bat_number", "pitch_number"],           │
│         how="left"                                                           │
│     )                                                                       │
│     # Statcast usa at_bat_number; feed usa at_bat_index — mismo concepto     │
│                                                                             │
│  5. Guardar: game_{pk}_{date}_pitches_enriched.parquet                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Resumen visual

| Fuente | Columnas |
|--------|----------|
| **Statcast** | ~79 (todo el schema KEEP excepto play_id) |
| **Feed** | 1 (play_id) |
| **Total pitches_enriched** | ~80 |

---

## RESUMEN FINAL

**TOTAL: 118 columnas Statcast + 1 del feed**

| Categoría | Columnas | % |
|-----------|----------|---|
| ✅ **KEEP - CORE** | **76** | **64%** |
| ├─ Identificadores & Contexto | 16 | (+ play_id del feed) |
| ├─ Pitch Characteristics | 14 | |
| ├─ Pitch Outcomes | 8 | |
| ├─ Batted Ball | 12 | (+ launch_speed_angle, xSLG) |
| ├─ Game State | 11 | |
| ├─ Advanced Metrics | 6 | |
| ├─ Optional Physics | 6 | |
| └─ Bat Tracking | 5 | (+ attack_direction, hyper_speed) |
| ❌ **DROP** | **41** | **35%** |

---

## Schema Recomendado: pitches_enriched.parquet

### Tier 1: ESSENTIAL (62 columnas)

**Identifiers (9):**
- game_pk, game_date, game_type, game_year
- pitcher, batter, player_name
- inning, inning_topbot

**Pitch Context (6):**
- at_bat_number, pitch_number, stand, p_throws
- home_team, away_team

**Pitch Stuff (11):**
- pitch_type, pitch_name
- release_speed, release_spin_rate
- release_extension, release_pos_x, release_pos_y, release_pos_z
- pfx_x, pfx_z, spin_axis

**Location (3):**
- plate_x, plate_z, zone

**Outcomes (5):**
- type, description, events, des, bb_type

**Count (2):**
- balls, strikes

**Batted Ball (12):**
- launch_speed, launch_angle, launch_speed_angle
- hit_distance_sc, hit_location, hc_x, hc_y
- estimated_woba_using_speedangle, estimated_ba_using_speedangle, estimated_slg_using_speedangle
- woba_value, woba_denom

**Game State (8):**
- home_score, away_score, home_score_diff
- outs_when_up, on_1b, on_2b, on_3b
- n_thruorder_pitcher

**Win Probability (6):**
- delta_home_win_exp, delta_run_exp, delta_pitcher_run_exp
- home_win_exp, bat_win_exp, babip_value

---

### Tier 2: ADVANCED (17 columnas opcionales)

**Physics:**
- effective_speed, sz_top, sz_bot
- api_break_z_with_gravity, api_break_x_arm, api_break_x_batter_in

**Defensive:**
- if_fielding_alignment, of_fielding_alignment

**Ages:**
- age_pit, age_bat

**Identifiers desde Feed (1):**
- play_id

**Bat Tracking (si disponible):**
- bat_speed, swing_length, attack_angle, attack_direction, hyper_speed

**Arm Angle:**
- arm_angle

---

## Implementación

### Opción A: Schema Fijo (Recomendado)

```python
COLUMNS_TO_KEEP = [
    # Identifiers (Statcast)
    'game_pk', 'game_date', 'game_type', 'game_year',
    'pitcher', 'batter', 'player_name',
    'inning', 'inning_topbot',
    # play_id viene del feed — se agrega en el merge

    # Context
    'at_bat_number', 'pitch_number', 'stand', 'p_throws',
    'home_team', 'away_team',

    # Pitch Stuff
    'pitch_type', 'pitch_name',
    'release_speed', 'release_spin_rate',
    'release_extension', 'release_pos_x', 'release_pos_y', 'release_pos_z',
    'pfx_x', 'pfx_z', 'spin_axis',

    # Location
    'plate_x', 'plate_z', 'zone',

    # Outcomes
    'type', 'description', 'events', 'des', 'bb_type',
    'balls', 'strikes',

    # Batted Ball
    'launch_speed', 'launch_angle', 'launch_speed_angle',
    'hit_distance_sc', 'hit_location', 'hc_x', 'hc_y',
    'estimated_woba_using_speedangle', 'estimated_ba_using_speedangle',
    'estimated_slg_using_speedangle',
    'woba_value', 'woba_denom',

    # Game State
    'home_score', 'away_score', 'home_score_diff',
    'outs_when_up', 'on_1b', 'on_2b', 'on_3b',
    'n_thruorder_pitcher',

    # Win Probability
    'delta_home_win_exp', 'delta_run_exp', 'delta_pitcher_run_exp',
    'home_win_exp', 'bat_win_exp', 'babip_value',

    # Advanced (optional)
    'effective_speed', 'sz_top', 'sz_bot',
    'if_fielding_alignment', 'of_fielding_alignment',
    'age_pit', 'age_bat', 'arm_angle',

    # Bat Tracking (keep if present)
    'bat_speed', 'swing_length', 'attack_angle', 'attack_direction', 'hyper_speed'
]

# En tu script de enrichment:
# 1. Statcast base
df_enriched = df_statcast[
    [col for col in COLUMNS_TO_KEEP if col in df_statcast.columns]
].copy()
# 2. Merge play_id desde feed (ver sección "Arquitectura del Join")
```

---

### Opción B: Schema Dinámico (Más flexible)

```python
# Drop deprecated y fielders (Opción B: schema dinámico)
COLUMNS_TO_DROP = [
    'tfs_deprecated', 'tfs_zulu_deprecated', 'umpire',
    'spin_dir', 'spin_rate_deprecated', 'break_angle_deprecated', 'break_length_deprecated',
    'sv_id', 'age_pit_legacy', 'age_bat_legacy', 'iso_value',
    'fielder_2', 'fielder_3', 'fielder_4', 'fielder_5',
    'fielder_6', 'fielder_7', 'fielder_8', 'fielder_9',
    'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az',
    'post_away_score', 'post_home_score', 'post_bat_score', 'post_fld_score',
    'bat_score', 'fld_score',
    'pitcher_days_since_prev_game', 'batter_days_since_prev_game',
    'pitcher_days_until_next_game', 'batter_days_until_next_game',
    'swing_path_tilt', 'n_priorpa_thisgame_player_at_bat',
    # intercept_ball_minus_batte* (nombres truncados en Statcast)
]

df_enriched = df_statcast.drop(
    columns=[col for col in COLUMNS_TO_DROP if col in df_statcast.columns]
)
# Drop intercept_ball_minus_* (swing path avanzado)
df_enriched = df_enriched.loc[:, ~df_enriched.columns.str.startswith('intercept_ball_minus')]
```

---

## Impacto en Storage

**Por juego (~255 pitches):**
- 118 columnas: 32KB
- 75 columnas: **~20KB** (-37%)
- 60 columnas: **~17KB** (-47%)

**Por temporada (2,430 juegos):**
- 118 columnas: ~78MB
- 75 columnas: **~49MB** (-37%)
- 60 columnas: **~41MB** (-47%)

**4 temporadas:**
- 118 columnas: ~312MB
- 75 columnas: **~196MB** (-37%)
- 60 columnas: **~164MB** (-47%)

---

## Mi Recomendación

**Usa Opción A con ~80 columnas (Tier 1 + Tier 2 + play_id).**

**Por qué:**
1. ✅ Mantiene toda la info útil para pitching analysis
2. ✅ Reduce storage 37% (de 312MB → 196MB para 4 años)
3. ✅ Elimina 100% del ruido (deprecated, fielders, vectores physics)
4. ✅ Flexible - incluye advanced metrics por si acaso
5. ✅ Bat tracking opcional (keep if present)

**Skip:**
- ❌ Schema de 60 columnas es muy agresivo (pierdes algunos advanced metrics que podrías necesitar)
- ❌ Schema de 118 columnas tiene mucho ruido innecesario

---

**¿Te gusta este approach?** ¿O quieres ajustar algo antes de implementarlo en tu pipeline de enrichment?
