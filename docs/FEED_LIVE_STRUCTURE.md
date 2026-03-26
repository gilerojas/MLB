# Raw feed_live JSON — Structure reference

**Source:** any `game_{pk}_{date}_feed_live.json`  
Use this to decide what can be moved to a per-season document (immutable or slow-changing) vs kept per game.

---

## Qué es lo que realmente pesa (tamaño en disco)

En un feed minificado (sin comprimir), el tamaño aproximado por componente:

| Componente | % del total | Qué es |
|------------|-------------|--------|
| **liveData.plays.allPlays** | **~63%** | Cada pitch: pitchData (coordinates, breaks, zone, speeds), details (call, type), count, startTime/endTime. Es lo que más pesa. |
| **liveData.boxscore** | **~26%** | Stats por jugador (batting/pitching del partido), teamStats, umpires (officials), battingOrder, batters/pitchers/bench/bullpen. |
| **gameData.players** | **~7%** | Bios del roster (fullName, height, weight, birthDate, position, strikeZoneTop/Bottom, etc.) — se repite en cada partido. |
| gameData.teams, venue, resto | ~0.5% | Muy pequeño. |
| liveData.linescore, decisions, leaders | ~0.4% | Muy pequeño. |

**Conclusión:** Casi todo el peso está en (1) **play-by-play** (pitches) y (2) **boxscore**. Si quieres reducir tamaño sin tocar lo que te interesa (umpires, weather, roster, cómo les fue), la única parte “movible” a un doc por temporada es **gameData.players** (~7%). El boxscore (umpires + stats) y el play-by-play son por partido y son lo que más ocupa; para hacerlos más ligeros haría falta guardarlos en otro formato (p. ej. Parquet) o recortar campos dentro de cada pitch.

**Si confías más en el feed que en Savant y no quieres dejar allPlays fuera:** allPlays es dato mutable y es el núcleo — no hay que omitirlo. La opción práctica: **seguir usando feed_live como fuente de verdad** y **derivar un Parquet desde allPlays** (y, si quieres, boxscore/officials). Misma fuente, misma información, formato más liviano y consultable. El runner diario (HR u otros) puede leer de ese Parquet; opcionalmente conservas el JSON raw como respaldo. Ver [STORAGE_STRATEGY.md](STORAGE_STRATEGY.md) y la fórmula recomendada en [PHASE0_STORAGE_FORMULA.md](PHASE0_STORAGE_FORMULA.md) (gzip + registry + parquet).

---

## Datos que te interesan: umpires, weather, roster del día y cómo les fue

Todo esto está en el feed; aquí se indica **dónde** está cada uno para que no lo dejes fuera al diseñar el almacenamiento.

### Umpires (árbitros)

**Ubicación:** `liveData.boxscore.officials`

Lista de árbitros del partido. Cada elemento tiene:
- **officialType**: `"Home Plate"`, `"First Base"`, `"Second Base"`, `"Third Base"`, `"Left Field"`, `"Right Field"`
- **official**: `{ "id", "fullName", "link" }`

Ejemplo:
```json
{ "officialType": "Home Plate", "official": { "id": 427248, "fullName": "Dan Iassogna", "link": "/api/v1/people/427248" } }
```

Los umpires son **por partido** (viven en liveData). Si guardas solo `liveData` por juego, ya los tienes.

---

### Weather (clima)

**Ubicación:** `gameData.weather`

Objeto con tres campos (string):
- **condition**: p. ej. `"Overcast"`, `"Clear"`
- **temp**: temperatura, p. ej. `"83"`
- **wind**: p. ej. `"10 mph, L To R"`

Ejemplo:
```json
{ "condition": "Overcast", "temp": "83", "wind": "10 mph, L To R" }
```

Es **por partido** pero está en gameData. Si separas gameData vs liveData, incluye al menos `gameData.weather` (y opcionalmente `gameData.game`, `datetime`) en lo que guardes por juego si te interesa el clima.

---

### Roster del día (quién jugó)

Dos piezas:

1. **Quién estaba en el partido (bios):** `gameData.players`  
   Diccionario `playerId → { fullName, birthDate, height, weight, primaryPosition, batSide, pitchHand, strikeZoneTop/Bottom, ... }`. Es el roster de ese juego con datos de jugador.

2. **Alineación y roles en el partido:** `liveData.boxscore.teams.away` y `liveData.boxscore.teams.home`  
   Por equipo:
   - **battingOrder**: lista de player IDs (orden al bate).
   - **batters**, **pitchers**, **bench**, **bullpen**: listas de player IDs.
   - **players**: diccionario `playerId → { person, jerseyNumber, position, status, battingOrder, stats }` — **cómo le fue a cada uno** (ver abajo).

Si quieres “roster del día” mínimo: con `liveData.boxscore.teams` tienes quién bateó/pitcheó y en qué orden; con `gameData.players` tienes nombres y datos de cada uno.

---

### Cómo les fue (estadísticas del partido)

**Por equipo (totales):** `liveData.boxscore.teams.away` / `liveData.boxscore.teams.home`  
- **teamStats.batting**: runs, hits, doubles, homeRuns, strikeOuts, avg, obp, slg, ops, plateAppearances, etc.  
- **teamStats.pitching**: runs, earnedRuns, inningsPitched, era, whip, strikeOuts, numberOfPitches, strikePercentage, etc.  
- **teamStats.fielding**: putOuts, assists, errors, chances, etc.

**Por jugador (en ese partido):** `liveData.boxscore.teams.away.players` / `liveData.boxscore.teams.home.players`  
Cada entrada tiene:
- **person**: id, fullName, link  
- **jerseyNumber**, **position** (code, name, type, abbreviation)  
- **battingOrder** (ej. `"601"`)  
- **stats.batting**: summary (ej. `"1-1 | HR, 3 RBI, R"`), gamesPlayed, runs, hits, atBats, homeRuns, rbi, etc.  
- **stats.pitching** (si pitcheó): inningsPitched, runs, earnedRuns, strikeOuts, etc.

**Resumen:** Umpires y “cómo les fue” (boxscore) están en **liveData**; weather está en **gameData**; roster del día es **gameData.players** (bios) + **liveData.boxscore.teams** (alineación y stats por jugador). Si guardas liveData completo por juego más un poco de gameData (p. ej. weather, game, datetime), no pierdes nada de lo que te interesa.

---

## Root

| Key | Type | Notes |
|-----|------|-------|
| `copyright` | string | Legal text. Can omit or keep once. |
| `gamePk` | number | Game ID. |
| `link` | string | API path. Can derive from gamePk. |
| `metaData` | object | Timestamp, game events. Optional. |
| `gameData` | object | **Largely immutable** — teams, players, venue. Can be one doc per season. |
| `liveData` | object | **Per game** — plays, linescore, boxscore. Keep per game. |

## gameData (immutable / per-season candidate)

Everything here is **game setup**: teams, roster, venue, weather, officials. Same players appear in many games; venue/weather are fixed for the game. Good candidate to store **once per season** (e.g. merged players + teams) and **omit from per-game raw**.

| Key | Description | Synthesize per season? |
|-----|-------------|------------------------|
| `alerts` | [ list of 0 items ] | Optional; can omit or keep in small game header. |
| `datetime` | dateTime, officialDate, dayNight, time | Per game; small. |
| `flags` | 6 keys | Optional; can omit or keep in small game header. |
| `game` | pk, type, season, doubleHeader, gameNumber | Per game but small; could keep in game header. |
| `gameInfo` | 3 keys | Optional; can omit or keep in small game header. |
| `moundVisits` | 2 keys | Optional; can omit or keep in small game header. |
| `officialScorer` | 3 keys | Optional; can omit or keep in small game header. |
| `officialVenue` | 2 keys | Optional; can omit or keep in small game header. |
| `players` | Roster: ID → fullName, birthDate, height, weight, primaryPosition, batSide, pitchHand, strikeZoneTop/Bottom, etc. | ✅ Yes — one master players doc per season. |
| `primaryDatacaster` | 3 keys | Optional; can omit or keep in small game header. |
| `probablePitchers` | 2 keys | Optional; can omit or keep in small game header. |
| `review` | 3 keys | Optional; can omit or keep in small game header. |
| `status` | abstractGameState, detailedState, statusCode | Per game; small. |
| `teams` | away/home: id, name, link, etc. | ✅ Yes — teams change rarely. |
| `venue` | id, name, location, timeZone, fieldInfo | ✅ Yes — venues are fixed. |
| `weather` | condition, temp, wind — clima del partido | **Guardar por juego** si te interesa (está en gameData). |

### gameData.players — one example (all keys)

These fields repeat for every player in every game. Storing them once per season saves space.

```
  active: false
  batSide: 2 keys
  birthCity: 'Wymore'
  birthCountry: 'USA'
  birthDate: '1987-01-21'
  birthStateProvince: 'NE'
  boxscoreName: 'Diekman'
  currentAge: 39
  draftYear: 2007
  firstLastName: 'Jake Diekman'
  firstName: 'Jacob'
  fullFMLName: 'Jacob Tanner Diekman'
  fullLFMName: 'Diekman, Jacob Tanner'
  fullName: 'Jake Diekman'
  gender: 'M'
  height: '6\' 4"'
  id: 518617
  initLastName: 'J Diekman'
  isPlayer: true
  isVerified: true
  lastFirstName: 'Diekman, Jake'
  lastInitName: 'Diekman, J'
  lastName: 'Diekman'
  lastPlayedDate: '2024-07-28'
  link: '/api/v1/people/518617'
  middleName: 'Tanner'
  mlbDebutDate: '2012-05-15'
  nameFirstLast: 'Jake Diekman'
  nameSlug: 'jake-diekman-518617'
  nickName: 'Gut It Out'
  pitchHand: 2 keys
  primaryNumber: '35'
  primaryPosition: 4 keys
  pronunciation: 'DEEK-man'
  strikeZoneBottom: 1.71
  strikeZoneTop: 3.388
  useLastName: 'Diekman'
  useName: 'Jake'
  weight: 195
```

### gameData.teams

```
  away: ['springLeague', 'allStarStatus', 'id', 'name', 'link', 'season', 'venue', 'springVenue', 'teamCode', 'fileCode', 'abbreviation', 'teamName', 'locationName', 'firstYearOfPlay', 'league', 'division', 'sport', 'shortName', 'record', 'franchiseName', 'clubName', 'active']
  home: ['springLeague', 'allStarStatus', 'id', 'name', 'link', 'season', 'venue', 'springVenue', 'teamCode', 'fileCode', 'abbreviation', 'teamName', 'locationName', 'firstYearOfPlay', 'league', 'division', 'sport', 'shortName', 'record', 'franchiseName', 'clubName', 'active']
```

## liveData (per game — keep)

Play-by-play, linescore, boxscore. **Changes every game**; keep per game (or flatten to parquet).

| Key | Description |
|-----|-------------|
| `plays` | allPlays, currentPlay, scoringPlays, playsByInning — the main pitch-by-pitch data. |
| `linescore` | currentInning, innings[], teams (runs by inning), balls/strikes/outs. |
| `boxscore` | teams (batting/pitching lines + **stats por jugador**), **officials** (umpires), info, topPerformers. |
| `decisions` | Win/loss/save (often empty until game ends). |
| `leaders` | hitDistance, hitSpeed, pitchSpeed — post-game. |

### liveData.plays.allPlays — one play (at-bat) example

**Top-level keys:** result, about, count, matchup, pitchIndex, actionIndex, runnerIndex, runners, playEvents, playEndTime, atBatIndex

```
  result:
      type: 'atBat'
      event: 'Walk'
      eventType: 'walk'
      description: 'Jackson Chourio walks.'
      rbi: 0
      awayScore: 0
      homeScore: 0
      isOut: false
  about:
      atBatIndex: 0
      halfInning: 'top'
      isTopInning: true
      inning: 1
      startTime: '2024-03-29T17:44:13.624Z'
      endTime: '2024-03-29T17:45:10.749Z'
      isComplete: true
      isScoringPlay: false
      ... and 3 more keys
  count:
      balls: 4
      strikes: 0
      outs: 0
  matchup:
      batter:
          id: 694192
          fullName: 'Jackson Chourio'
          link: '/api/v1/people/694192'
      batSide:
          code: 'R'
          description: 'Right'
      pitcher:
          id: 500779
          fullName: 'Jose Quintana'
          link: '/api/v1/people/500779'
      pitchHand:
          code: 'L'
          description: 'Left'
      postOnFirst:
          id: 694192
          fullName: 'Jackson Chourio'
          link: '/api/v1/people/694192'
      batterHotColdZones: [ list of 0 items ]
      pitcherHotColdZones: [ list of 0 items ]
      splits:
          batter: 'vs_LHP'
          pitcher: 'vs_RHB'
          menOnBase: 'Men_On'
  pitchIndex: [ list of 4 items ]
  actionIndex: [ list of 3 items ]
  runnerIndex: [ list of 1 items ]
    [0] 0
  runners: [ list of 1 items ]
    [0] 3 keys
        movement:
            originBase: null
            start: null
            end: '1B'
            outBase: null
            isOut: false
            outNumber: null
        details:
            event: 'Walk'
            eventType: 'walk'
            movementReason: null
            runner:
                id: 694192
                fullName: 'Jackson Chourio'
                link: '/api/v1/people/694192'
            responsiblePitcher: null
            isScoringEvent: false
            rbi: false
            earned: false
            ... and 2 more keys
        credits: [ list of 0 items ]
  playEvents: [ list of 7 items ]
  playEndTime: '2024-03-29T17:45:10.749Z'
  atBatIndex: 0
```

### liveData.plays.allPlays[].playEvents[] — one pitch example

**Top-level keys:** details, count, pitchData, index, playId, pitchNumber, startTime, endTime, isPitch, type

```
  details:
      call: 2 keys
      description: 'Ball'
      code: 'B'
      ballColor: 'rgba(39, 161, 39, 1.0)'
      trailColor: 'rgba(50, 0, 221, 1.0)'
      isInPlay: false
      isStrike: false
      isBall: true
      ... and 3 more keys
  count:
      balls: 1
      strikes: 0
      outs: 0
  pitchData:
      startSpeed: 90.6
      endSpeed: 80.6
      strikeZoneTop: 3.49
      strikeZoneBottom: 1.6
      coordinates: 15 keys
      breaks: 8 keys
      zone: 14
      typeConfidence: 0.9
      ... and 2 more keys
  index: 3
  playId: '75265697-70c4-43f0-97cb-1a45aeffddab'
  pitchNumber: 1
  startTime: '2024-03-29T17:44:14.624Z'
  endTime: '2024-03-29T17:44:17.624Z'
  isPitch: true
  type: 'pitch'
```

**pitchData sub-keys:** startSpeed, endSpeed, strikeZoneTop, strikeZoneBottom, coordinates, breaks, zone, typeConfidence, plateTime, extension

**pitchData.coordinates:** aY, aZ, pfxX, pfxZ, pX, pZ, vX0, vY0, vZ0, x, y, x0, y0, z0, aX

**pitchData.breaks:** breakAngle, breakLength, breakY, breakVertical, breakVerticalInduced, breakHorizontal, spinRate, spinDirection

**details sub-keys:** call, description, code, ballColor, trailColor, isInPlay, isStrike, isBall, type, isOut, hasReview

**hitData** (present when the pitch is put in play, `details.isInPlay`): exit velocity and launch angle from the feed.
- **launchSpeed** — exit velocity (mph).
- **launchAngle** — launch angle (degrees).
- **totalDistance** — projected distance (feet).
- **trajectory** — e.g. "ground_ball", "fly_ball".
- **hardness** — e.g. "medium", "hard".
- **location** — field location (e.g. "4" for second base).
- **coordinates** — coordX, coordY (landing position).

So you **do** get exit velo and launch angle from the live feed; flatten and use them (e.g. as launch_speed_feed, launch_angle_feed) to compare with Statcast or as feed-sourced hit metrics.

---

## Summary: what to leave out per game

| Store once per season | Omit or keep minimal per game |
|------------------------|----------------------------------|
| **gameData.players** (roster bios) | **liveData** (plays, linescore, boxscore) — keep per game or as parquet |
| **gameData.teams** (away/home info) | **gameData.game** (pk, type, season) — tiny; can keep in game header |
| **gameData.venue** | **gameData.datetime**, **status** — small |
| **gameData.weather** (si te interesa el clima) | Guardar por juego junto con liveData o en un header mínimo. **metaData**, **copyright**, **link** — omit o derivar. |
