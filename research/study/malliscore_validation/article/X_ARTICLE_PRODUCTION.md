# Building MalliScore - X Article Production Draft

## Article

**Title:** Building MalliScore: A Modern Way to Evaluate Pitching Performance

**Deck:** Identical Game Scores can hide different performances. MalliScore asks how the outing was built.

**Body:** [ARTICLE.md](ARTICLE.md)

The article body is the master copy for X, Medium, and LinkedIn. Platform introductions
and calls to action may change later; the analytical body should remain identical.

## Header image

Upload as the X Article cover:

`research/study/malliscore_validation/article/assets/malliscore_article_cover.png`

The cover is a generated editorial illustration. It uses the official Mallitalytics
stacked logo as a reference and contains no generated statistics.

## Evidence image order

### 1. Same line, different path

Upload directly after the Imanaga-Wacha comparison table.

`research/study/malliscore_validation/article/assets/05_same_line.png`

Evidence shown: Imanaga and Wacha recorded identical 7.0 IP, 4 H, 1 BB, 0 ER, 7 K
lines and identical Game Score v2 values, but differed sharply in swing-and-miss and
chase. This is the article's Notice moment: different, equally valid paths to clean run
prevention, with one outing also showing stronger active lineup control.

### 2. Score architecture

Upload after the paragraph ending:

> The 100-point ceiling is theoretical, not a target we expect normal starts to reach.

Asset:

`research/study/malliscore_validation/article/assets/01_score_architecture.png`

Evidence shown: the exact current structure of MalliScore: the two weighted pillars,
their harmonic-mean core, and the outs-first workload adjustment.

### 3. Weight sensitivity

Upload after the paragraph ending:

> Pretending the data had discovered one perfect set of weights would have created false precision.

Asset:

`research/study/malliscore_validation/article/assets/03_weight_sensitivity.png`

Evidence shown: 20,000 feasible weight vectors; minimum Spearman agreement .963; 97%
of vectors above .980.

### 4. Game Score disagreement

Upload after the two bullets comparing MalliScore-favored and Game Score-favored starts.

Asset:

`research/study/malliscore_validation/article/assets/02_game_score_disagreement.png`

Evidence shown: the clearest 2024 disagreement cases differ systematically in swinging
strikes, innings, and earned runs.

### 5. Predictive boundary

Upload after the paragraph stating that neither metric adds meaningful next-start signal.

Asset:

`research/study/malliscore_validation/article/assets/04_next_start_signal.png`

Evidence shown: all four MalliScore-over-Game-Score next-start tests are NULL_CONFIRMED;
the confidence intervals remain inside the predeclared practical-effect region.

## Publishing checks

- Confirm the title is not truncated in the X Article preview.
- Confirm the cover uses the complete generated image without an automatic crop.
- Add image alt text from the descriptions in `ARTICLE.md`.
- Keep the formula in a preformatted text block.
- Verify the FanGraphs and MLB Game Score links.
- Preview on mobile before publishing.
- Do not publish the launch post until the Article URL exists.

## Communication after publication

The launch post and any baseball-community post should be written after the final X
Article preview is approved. Their job is to create curiosity and route readers to the
article, not to summarize all four findings.
