# The Malli Postseason Diagnostic

## Working brief

Prepared October 1, 2026, before the Phillies-Braves Wild Card deciding game.
Status: preliminary research, not final predictions or publication-ready team profiles.

## The question

**Which teams have the strongest paths to advancing, and which strengths or weaknesses could decide those paths?**

The standings give us a starting point. October concentrates more work in the hands of a team's best available players. The question is whether those players can handle the opponent in front of them.

The review should help a reader see why a team might advance and what to watch once the game starts: the starter who can escape trouble, the reliever a manager trusts with a one-run lead, or the hitter who can change the score with one swing.

Keep the author's team allegiance private. Milwaukee is an early interest, not an approved championship pick. Leave final picks for the author to approve.

## Four questions behind the review

### Can the starters get outs without needing a defensive play?

Start with the likely series starters' K-BB%: strikeout percentage minus walk percentage. Add contact allowed when it helps explain a strength or a concern. This keeps attention on pitchers who can end a plate appearance themselves while limiting free baserunners. Pitchers who induce weak contact can succeed too; their case needs room in the review.

For the reader: who can stop a rally before another ball goes into play?

### Who gets the important outs after the starter leaves?

FanGraphs' comparison found that relievers handled a larger share of postseason innings than regular-season innings in every year from 2015 through its 2022 publication cutoff. The 2022 postseason was still in progress. That makes the available bullpen worth close attention; it does not establish bullpen quality as a stand-alone predictor of advancement. [Jay Jaffe's bullpen analysis](https://blogs.fangraphs.com/less-is-more-relief-pitching-has-dominated-this-postseason/).

Identify the three or four relievers the manager is most likely to trust. Check their K-BB%, contact allowed, and recent usage. Full-season bullpen ERA can include pitchers who no longer have those jobs. Separate recorded workload from confirmed availability; a pitch count alone cannot establish fatigue or injury.

For the reader: can this team protect a narrow lead without running out of reliable arms?

### Can the lineup create damage and get on base?

A second FanGraphs analysis found that home runs supplied a higher share of postseason runs in each year from 2015 through its November 2022 cutoff. This describes how runs were scored. It does not show that the regular-season home-run leader was most likely to advance. [Jay Jaffe's postseason power analysis](https://blogs.fangraphs.com/no-hitters-are-great-but-the-long-ball-still-wins-in-october/).

Look at power production, batting K%, and on-base ability together. A home run can rescue an inning with few opportunities. Contact can keep an opportunity alive, but its quality matters. Use the expected lineup to decide which of those strengths a team can bring into its series.

For the reader: can they score without needing three or four consecutive things to go right?

### Does the strong finish tell us something new?

Use the final 30 played games as context. Then check whether a player returned, a role changed, or the players expected to appear in October improved. A winning streak alone cannot tell us whether the current team is stronger.

Milwaukee and San Diego give us a useful test. Both finished well. Milwaukee had the larger full-season run differential. San Diego's case needs an explanation of what changed and why it matters against Milwaukee.

These studies guide the questions. They do not supply playoff probabilities or weights for a new score. Defense, health, and series scheduling enter the discussion where they could change a specific matchup.

## Scope and bracket

The Wild Card Series is part of the postseason, not a preseason round. Frame the main publication as a Division Series preview, supported by an appendix covering all 12 original qualifiers.

Seven teams have secured Division Series places at this research cutoff. Atlanta and Philadelphia are competing for the eighth; maintain separate conditional profiles until their deciding game is final. Houston, Boston, and the Cubs belong in a retrospective section, not among current picks.

Division Series pairings:

- White Sox vs. Guardians.
- Yankees vs. Rays.
- Padres vs. Brewers.
- Phillies or Braves vs. Dodgers.

The deciding Wild Card game is scheduled for 8 p.m. ET on October 1. Division Series play begins October 3. [Official MLB schedule](https://www.mlb.com/news/2026-mlb-playoff-and-world-series-schedule).

The supplied bracket image establishes the original field and seeding. It does not establish current injuries, results, or series pitching plans.

## Verified baseline

Regular-season results through September 27. Recent form means each team's final 30 **played regular-season games**, not calendar September and not the Wild Card Series.

| Team | Seed | Season W-L | Run differential | Final 30 W-L | Final 30 run differential | Batting OPS | Pitching ERA |
|---|---:|---:|---:|---:|---:|---:|---:|
| Brewers | NL 1 | 103-59 | +214 | 22-8 | +54 | .744 | 3.51 |
| Dodgers | NL 2 | 100-62 | +201 | 20-10 | +51 | .762 | 3.55 |
| Braves | NL 3 | 94-68 | +116 | 17-13 | +5 | .718 | 3.63 |
| Padres | NL 4 | 91-71 | +41 | 20-10 | +26 | .717 | 3.99 |
| Cubs | NL 5 | 89-73 | +147 | 13-17 | +25 | .767 | 4.12 |
| Phillies | NL 6 | 88-74 | +15 | 15-15 | -5 | .707 | 4.04 |
| Rays | AL 1 | 98-64 | +86 | 20-10 | +42 | .728 | 3.70 |
| Guardians | AL 2 | 85-77 | +11 | 19-11 | +18 | .696 | 3.77 |
| Astros | AL 3 | 81-81 | -32 | 15-15 | +10 | .732 | 4.45 |
| Yankees | AL 4 | 93-68 | +138 | 19-11 | +40 | .720 | 3.23 |
| Red Sox | AL 5 | 87-75 | +78 | 14-16 | -27 | .717 | 3.54 |
| White Sox | AL 6 | 84-78 | +56 | 15-15 | +18 | .726 | 4.12 |

Sources: VPS warehouse schedule for results and recent windows; official MLB team season-stat endpoints for OPS and ERA. OPS and ERA here are descriptive, unadjusted for park or opponent. Do not label them wRC+, FIP, or expected performance.

API team statistics query: `https://statsapi.mlb.com/api/v1/teams/stats?sportId=1&season=2026&stats=season&group=hitting,pitching&gameType=R`.

### Coverage and the cancelled game

The warehouse has raw and enriched files for all 162 played games of each of these teams except the Yankees, who played 161. Their September 27 game against Baltimore was cancelled for rain, gamePk 823490. MLB's schedule labels it abstractGameState Final but detailedState Cancelled, with no winner or scores.

Do not filter only on abstractGameState. Require a played result, valid scores, and winner information. Removing the cancellation changes the Yankees' apparent last-30 window from 29 played games to the correct 19-11 over 30. Their 93-68 season record is corroborated by the official AL standings.

This audit established file existence, not full pitch-level completeness or the validity of every derived metric. Before publishing Statcast season rankings, check row counts, duplicates, tracking coverage, and PA definitions.

## Initial team reads

These are hypotheses grounded in the baseline, not final series calls. The historical research above is now part of the approach. The player-level checks are still pending: likely starters, trusted relievers, their contact profiles, and current availability. Staff-wide statistics below cannot substitute for those checks.

### Milwaukee Brewers

Milwaukee's +214 differential and 22-8 finish give us a strong starting case. Its 668 walks suggest a lineup that creates opportunities even when the ball stays in the park.

Research question: can the expected postseason lineup keep creating baserunners against San Diego's available arms, and how much of the season pitching advantage belongs to the pitchers who will actually work this series?

Watch: baserunner creation before the middle of the order and how Milwaukee covers innings after its listed starters. Do not infer a specific bullpen strength from team ERA alone.

### Los Angeles Dodgers

A .762 OPS and +201 differential give the Dodgers a broad season case. Their staff had a 17.6% K-BB%, calculated from 1,521 strikeouts and 480 walks over 5,921 batters faced. We still need to establish how much of that performance belongs to the pitchers available for this series.

Research question: which of those arms and hitters are available and in what roles? Recheck current health and pitching plans instead of treating the full-season roster as the NLDS roster.

Watch: the actual starter sequence and high-leverage relief options against whichever opponent advances. Keep the Phillies and Braves matchup branches separate.

### Tampa Bay Rays

The AL's top seed finished 20-10 with a +42 differential. The pitching staff issued 409 walks in 5,939 batters faced, a 6.9% walk rate. That is a useful control indicator, not proof of command in every pitch location.

Research question: can their available staff limit free baserunners against New York without exposing its home-run vulnerability? Tampa Bay allowed 201 regular-season home runs; the Yankees hit 223.

Watch: who receives those innings, their own home-run/contact profiles, and the handedness of the hitters they face. Team totals only flag the question.

### New York Yankees

New York's +138 differential exceeds Tampa Bay's +86. The Yankees hit 223 home runs, but also struck out in 24.7% of their plate appearances. That gives the matchup a clear question: how often can their power reach a staff that limits walks?

Research question: will the expected lineup's power compensate for its missed contact against the Rays' actual postseason staff? Assess how Wild Card usage changes the first two games' pitching options.

Watch: whether lineup availability changes the season offensive profile. Do not assume a star is active merely because his season totals are included.

### Cleveland Guardians

The 19-11 finish is encouraging, but an 85-77 record and +11 differential do not establish a large season-quality gap over Chicago. Cleveland's .696 OPS and 162 home runs contrast with its pitching staff's 1,530 strikeouts.

Research question: how much bat-missing can the actual series rotation and bullpen supply, and where does the expected lineup create damage?

Watch: whether the innings assigned to their best available arms offset the offensive difference. Verify the starter announcements rather than assuming the bye guarantees a particular rotation.

### Chicago White Sox

The White Sox won one fewer regular-season game than Cleveland but had a +56 differential and 211 home runs. Their power gives us a possible upset case. Their 4.12 team ERA leaves a pitching question that needs to be answered through the available arms.

Research question: can their power reach Cleveland's specific arms often enough, and can their available pitching prevent the extra baserunners that erase that advantage?

Watch: power versus missed contact, starter rest, and relief availability after the Wild Card round.

### San Diego Padres

San Diego finished 20-10. Its +41 season differential is far below Milwaukee's +214, so the recent record needs an explanation before it can carry an upset pick.

Research question: did personnel, usage, or underlying performance change during the strong finish? If so, how much of that change carries into the Milwaukee matchup?

Watch: the recent performance of the players actually expected to play. Separate new information from a short winning streak.

### Atlanta Braves, conditional on advancing

Atlanta's 94 wins and +116 differential provide a legitimate foundation. The final 30 games produced only a +5 differential despite a 17-13 record, so avoid describing that finish as unequivocal dominance.

Research question: what does the deciding Wild Card game cost in starter and bullpen availability for Saturday? A projected NLDS roster and rotation must remain conditional.

Watch: the winner's available innings, not just its season ace names. Do not lower a team's talent assessment solely because a deciding game required more work; reflect that work in the schedule-specific matchup.

### Philadelphia Phillies, conditional on advancing

A +15 differential, .707 OPS, and a 15-15 finish do not support a broad hottest-team argument. However, the staff struck out 1,577 and walked 484 over 6,124 BF, a 17.8-point K-BB%. That contrasts with a 4.04 ERA and deserves decomposition by pitcher and role.

Research question: can the available postseason arms retain that bat-missing while limiting damaging contact, and can the current lineup supply enough offense?

Watch: who actually produced those strikeouts and who is available after Game 3. A team aggregate is not an NLDS pitching projection.

### The eliminated qualifiers

**Cubs:** .767 OPS and +147 differential made a substantive regular-season case. A 13-17 finish with a positive +25 differential warns against equating recent W-L with uniform poor performance. Their 231 home runs allowed merit investigation. Do not retroactively call them an obvious failure because a short series ended badly.

**Red Sox:** a 3.54 team ERA and +78 differential coexist with a 14-16, -27 finish. Investigate the available roster and the causes of the late decline; do not claim the decline caused the postseason elimination without examining the games.

**Astros:** 226 home runs and a .732 OPS contrast with 650 pitching walks, a 4.45 ERA, and a -32 differential. That is a clear regular-season strength/risk split. The retrospective should test whether the actual series reflected it, not assume that it did.

## The published team profiles

Each profile should answer four things: why I believe in them, what worries me, what could decide their series, and my pick. Two or three meaningful numbers should usually be enough. Explain what each number changes about the baseball argument.

Write short paragraphs with varied sentence lengths. Use the four questions to organize the research without forcing four identical headings onto every team. Prefer a concrete viewing cue over another paragraph of metrics. The final profiles should feel like someone explaining their pick to another baseball fan.

Follow [the Malli writing guide](../../docs/anti-ai-writing-style.md): no em dashes, canned transitions, manufactured contrasts, or recap ending. Keep real uncertainty visible. Do not invent personal observations or final predictions to give the prose personality.

MalliScore can support a description of recent outings. It has not been validated as a team playoff forecast, so do not turn it into advancement probabilities.

## Picks still to make

Keep separate fields for season strength, current availability, matchup lean, and uncertainty. Do not compress everything into an arbitrary new weighted score.

Initial shortlist for further evaluation, not final picks:

- Broad favorite candidates: Milwaukee and Los Angeles.
- AL contenders requiring direct comparison: Tampa Bay and New York.
- Upset candidate worth serious testing: Chicago White Sox.
- Strong finish to investigate rather than assume predictive: San Diego.
- Conditional dossiers: Atlanta and Philadelphia.

Cleveland remains a live contender and has the bye/home-field advantages; the shortlist above is not a complete probability ranking. Favorites here means our analytical assessment, not verified betting-market favoritism. Reserve percentages for a separately validated and documented forecasting model.

## What remains before publication

The baseline and historical research are in place. Finish these checks without expanding this into a new historical forecasting study:

- Confirm the last Wild Card result, series starters, rosters, and injury reports. Date the availability check.
- Examine K-BB% and contact allowed for the likely starters and three or four trusted relievers per team. Check relief usage from the Wild Card round. Label samples and missing tracking data.
- Check power, batting K%, and on-base ability for the expected lineup. Investigate one personnel or role change when a strong finish is part of the argument.
- Put the best contrary evidence beside each proposed pick. Get the author's final calls before publishing.

Use a reporting detail only when it changes the case. Attribute it and distinguish an announced plan from a possible one. Extra charts, new scores, and a full audit of every roster statistic are outside this pass.

Useful reporting leads, not yet incorporated as verified current roster claims:

- [Dodgers postseason FAQ](https://www.mlb.com/dodgers/news/dodgers-2026-postseason-faq).
- [Brewers postseason FAQ](https://www.mlb.com/brewers/news/brewers-2026-postseason-faq).
- [White Sox-Guardians Game 1 guide](https://www.mlb.com/guardians/news/white-sox-vs-guardians-alds-game-1-starting-lineups-pitching-matchup-2026).
- [Yankees-Rays Game 1 guide](https://www.mlb.com/yankees/news/yankees-vs-rays-alds-game-1-starting-lineups-pitching-matchup).
- [Padres-Brewers Game 1 guide](https://www.mlb.com/padres/news/padres-brewers-nl-division-series-game-1-starting-lineups-and-pitching-matchup).

## Publication plan

One master Division Series diagnostic, with short standalone team excerpts on X. Build a neutral comparison table or graphic after the roster-adjusted research is finished; no need for 12 new full player-card-style graphics.

Freeze the original picks and data cutoff. Update availability and results in clearly labeled additions rather than silently rewriting the original judgment. After each series, revisit which parts of the case held up, not merely whether the pick won.

No production automation, publishing rules, or schedules were changed for this research.
