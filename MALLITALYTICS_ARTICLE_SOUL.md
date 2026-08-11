# Mallitalytics Article Soul

The single source of truth for planning, researching, drafting, reviewing, and publishing
Mallitalytics long-form articles.

Read this document after `MALLITALYTICS_X_BRAND_SOUL.md` and before writing an article.
Use `MALLITALYTICS_BRAND.md` when producing its cover, charts, tables, or other visuals.

Last updated: August 2026

---

## 1. Why Mallitalytics Publishes Articles

Mallitalytics articles help curious baseball fans see something they had not noticed,
understand why it matters, and know what to watch next.

An article is not a longer tweet. It earns its length by doing at least one job that a
short post cannot do well:

- establish a meaningful baseball question;
- connect several pieces of evidence;
- explain a method without hiding its tradeoffs;
- test an interpretation against alternatives;
- preserve an original piece of Mallitalytics research.

The reader should finish with a better baseball lens, not merely more information.

### The article promise

> Find the signal, explain it clearly, and leave the reader better prepared to watch
> baseball.

---

## 2. The Governing Editorial Spine

Every article follows the Mallitalytics editorial operating system:

```text
NOTICE -> EXPLAIN -> WATCH
```

### Notice

Reveal the overlooked pattern, contradiction, limitation, or question.

Complete this sentence before drafting:

> What I want the reader to notice is: ________________________________.

If the answer is merely a statistic, the idea is not ready. The Notice must explain why
the statistic changes the way we see a player, game, method, or baseball question.

### Explain

Build the case with verified evidence and baseball context. Define the measurement,
show the mechanism, compare plausible alternatives, and distinguish observation from
interpretation.

### Watch

Return the analysis to baseball. Tell the reader where the idea can be applied, tested,
challenged, or noticed again. A Watch is not a forced prediction.

---

## 3. The Thesis Test

Every analytical article must be reducible to this argument:

> **The conventional view says X. That misses Y. The evidence shows Z. This matters
> because W.**

This is the most important planning line in the system.

- **X - Conventional view:** What would a reasonable fan believe before reading?
- **Y - Missing layer:** What does that view overlook, flatten, or misclassify?
- **Z - Evidence:** What verified finding changes the read?
- **W - Meaning:** Why should a baseball fan care?

The formula creates tension without manufacturing controversy. X does not need to be
stupid or wrong. The strongest Mallitalytics work often shows that the familiar view is
useful but incomplete.

### Thesis quality rules

A publishable thesis is:

- specific enough to be challenged;
- supported by the available evidence;
- meaningful in baseball terms;
- honest about scope and uncertainty;
- different from a summary of the data.

Avoid theses that amount to "Player X is good," "this statistic is interesting," or
"the model works." Those are conclusions without a useful tension.

---

## 4. Reader Value

Before drafting, select the primary value and no more than one secondary value.

| Value | Reader reaction | Article job |
|---|---|---|
| Discovery | "I had not noticed that." | Reveal a hidden pattern or disagreement. |
| Clarity | "Now I understand why." | Translate a mechanism, metric, or tradeoff. |
| Anticipation | "Now I know what to watch." | Give the reader a reusable baseball lens. |
| Reference | "I know where to return for this." | Preserve a method, definition, or study. |

Trying to provide all four equally usually produces an unfocused article.

---

## 5. The Article Brief

AI must not draft an article until this brief is complete.

```text
WORKING TITLE:
ARTICLE TYPE:
PRIMARY AUDIENCE:
READER QUESTION:

NOTICE:
What I want the reader to notice is...

THESIS:
The conventional view says X.
That misses Y.
The evidence shows Z.
This matters because W.

PRIMARY READER VALUE:
SECONDARY READER VALUE (optional):

SCOPE:
What this article evaluates...

BOUNDARY:
What this article does not establish...

EVIDENCE NEEDED:
-
-
-

BEST REAL EXAMPLE:
BEST COUNTEREXAMPLE OR DISAGREEMENT CASE:
HUMAN CONTEXT BEAT:

VISUALS:
1.
2.
3.

WHAT TO WATCH:
What should the reader notice in future games or analysis?
```

If the evidence needed is unavailable, narrow the thesis before writing. Never expand a
claim to compensate for thin data.

---

## 6. The Default Article Architecture

Use this as a spine, not a rigid set of visible headings.

### 1. The baseball moment

Open with a real outing, player, pitch, decision, disagreement, or familiar fan
experience. Give the reader baseball before methodology.

### 2. The conventional read

State the reasonable surface interpretation. Represent it fairly.

### 3. The missing layer

Introduce the tension: what the normal interpretation cannot explain. This is the
Notice moment and the article's reason to exist.

### 4. The question

Ask the precise question the article will answer. Define important terms in plain
baseball language.

### 5. The evidence

Move from the clearest evidence to the more technical evidence. Each chart, table, and
number must advance the argument rather than decorate it.

### 6. The mechanism

Explain why the pattern may exist, how the metric works, or which baseball tradeoff is
being measured. Clearly label inference and reporting.

### 7. The comparison

Test the idea against the strongest reasonable alternative, not a weak straw man. For a
new metric, explain what existing metrics still do well.

### 8. The real example

Walk through one complete case. Put formulas immediately next to real numbers and
translate the output back into baseball language.

### 9. The limits

State what the evidence cannot support, where the method can fail, and which questions
remain unresolved. Limitations increase trust when they are concrete.

### 10. What to watch

Return to the opening baseball question. Give the reader a practical lens, unresolved
tension, or future test. End with meaning, not a generic engagement request.

---

## 7. Structures By Article Type

All types use the same thesis test and NOTICE -> EXPLAIN -> WATCH spine. Their emphasis
changes.

### A. Metric or Method Article

```text
baseball problem
-> what existing measures capture
-> what remains missing
-> metric purpose
-> formula in plain English
-> worked example
-> validation and comparisons
-> failure cases and limitations
-> how to use it
```

Required: definition, scale, inputs, worked example, comparison, validation, limitation,
and explicit statement of whether the metric is descriptive or predictive.

### B. Player or Team Analysis

```text
surprising result
-> surface explanation
-> underlying change or tension
-> evidence across multiple views
-> baseball mechanism
-> counterpressure or risk
-> what to watch next
```

Required: a real change, comparison baseline, sample context, and one competing
explanation.

### C. Research Question or League Trend

```text
question
-> why the question matters
-> data and definitions
-> result
-> robustness checks
-> baseball interpretation
-> exceptions
-> implications
```

Required: population, date range, exclusions, comparison method, uncertainty, and a
separation between result and interpretation.

### D. Historical or Context Article

```text
present-day hinge
-> historical question
-> period context
-> evidence or comparison
-> what changed
-> what did not change
-> why the history matters now
```

Required: dated sources, era context, and protection against comparing raw statistics
across unlike environments without adjustment.

---

## 8. Evidence Standard

Mallitalytics treats data as evidence, not decoration.

### Source order

1. Mallitalytics warehouse and verified Statcast calculations.
2. MLB Stats API, Baseball Savant, MLB glossary, and official records.
3. Primary reporting, player or coach quotes, and team reporting.
4. Reputable secondary analysis for context or competing interpretations.

For a player, outing, or current trend, use warehouse/Statcast first and web reporting
for one human hinge: an injury return, pitch-design change, role change, mechanical
adjustment, or relevant quote.

### Claim ledger

Every factual or analytical claim must be assigned one status before drafting:

| Status | Meaning | Treatment |
|---|---|---|
| DATA | Directly calculated or observed | State precisely with dates and sample. |
| REPORTED | Attributed to a source | Link and attribute it. |
| INFERENCE | Interpretation supported by evidence | Use measured language. |
| UNKNOWN | Plausible but unsupported | Remove it or turn it into a question. |

### Non-negotiable integrity rules

- Never invent a statistic, quote, source, date, or example.
- Never let AI fill a missing value with a plausible number.
- Do not imply causation from correlation or timing alone.
- State the denominator, date range, and meaningful qualification rules.
- Use the strongest relevant counterexample, not only favorable examples.
- Distinguish descriptive metrics from predictive models.
- Preserve null results when they clarify what a method cannot do.

---

## 9. Making Technical Baseball Clear

Use this order whenever introducing a metric or model:

```text
baseball reason -> plain-English definition -> formula -> real example -> interpretation
```

Do not lead with notation.

### Formula rule

Every formula must answer four questions nearby:

1. What enters it?
2. Why does each component belong?
3. Which direction is better?
4. What does the resulting number mean?

### Comparison rule

Do not introduce a new metric by pretending old metrics are useless.

Explain:

- what the existing measure was designed to do;
- what it still does well;
- where the new method asks a different question;
- what disagreement between them teaches us.

### Worked-example rule

At least one technical article must let a reader follow a real case from inputs to
output. Round values only when disclosed and keep the exact calculation available in
the research record.

---

## 10. Voice And Reading Experience

The voice is warm, analytical, curious, and baseball-native. It should feel like a smart
fan explaining something carefully to another smart fan.

### Write like this

- Use concrete baseball language before statistical terminology.
- Prefer confident precision over academic distance.
- Let tension come from evidence, not hype.
- Vary sentence length while keeping paragraphs compact.
- Use "we" when describing Mallitalytics decisions or research.
- Admit uncertainty directly without burying the article in caveats.

### Avoid

- "In today's data-driven landscape..."
- "Let's dive in."
- "It is important to note..."
- generic superlatives and fake surprise;
- acronym-heavy opening paragraphs;
- repeating the thesis after every section;
- presenting the writer's process as more important than the baseball question;
- conclusions that merely restate the chart.

### X Article reading rules

- Make the first 150 words deliver the moment, tension, and reader promise.
- Keep most paragraphs to one to three sentences.
- Use a meaningful subheading every three to five paragraphs.
- Put one central idea in each section.
- Use bold sparingly for the sentence a skimming reader must retain.
- Break technical sections with a chart, example, table, or short summary.
- Write the title and opening for the intended baseball reader, not for an algorithm.

---

## 11. Visual Contract

Article visuals present evidence. The prose interprets it.

A standard analytical article should normally contain:

1. **Cover image:** title, subject, one visual signal, and restrained branding.
2. **Core evidence visual:** the chart or comparison that carries the thesis.
3. **Worked example:** a compact table, formula card, or annotated case.
4. **Optional robustness visual:** distribution, sensitivity, disagreement, or limits.

Do not force four visuals when two explain the idea better.

Every visual must:

- add evidence not already obvious from the nearby paragraph;
- define the metric and comparison;
- show the data-through date and source where relevant;
- follow `MALLITALYTICS_BRAND.md`;
- remain legible on a mobile screen;
- avoid placing the article's conclusion inside the chart.

---

## 12. AI Production Protocol

AI can perform most of the drafting, but it does not own the thesis or the truth.

### Pass 1: Research packet

Collect verified data, formulas, sources, definitions, real examples, counterexamples,
limitations, and unresolved questions. Do not write polished prose yet.

### Pass 2: Editorial brief

Complete the Article Brief. Produce three possible theses and choose the strongest one
based on reader value and evidence, not novelty alone.

### Pass 3: Claim ledger

List every substantive claim with its DATA, REPORTED, INFERENCE, or UNKNOWN status and
supporting source. Delete or reframe UNKNOWN claims.

### Pass 4: Outline

Assign one job to every section. The outline must visibly move from Notice to Explain to
Watch. A section without a distinct job is removed or merged.

### Pass 5: Draft

Draft only from the approved packet and ledger. Use placeholders such as `[VERIFY]` or
`[EXAMPLE NEEDED]` rather than inventing missing information.

### Pass 6: Adversarial review

Ask:

- What is the strongest reasonable objection?
- Is the thesis larger than the evidence?
- Is one variable being counted twice?
- Are sample size, selection, or era effects distorting the result?
- Could the same evidence support another explanation?
- Does the article state what the method cannot do?

### Pass 7: Clarity and voice

Remove jargon, repetition, throat-clearing, generic AI phrasing, and unnecessary
methodological detail. Preserve the evidence and uncertainty.

### Pass 8: Human approval

The founder must approve:

- the Notice;
- the thesis;
- the strongest interpretation;
- the limitations;
- the final title and opening;
- every claim that defines Mallitalytics' public position.

The human role is editorial judgment, not merely correcting grammar.

---

## 13. Article Readiness Scale

Score each dimension from 0 to 2.

| Dimension | 0 | 1 | 2 |
|---|---|---|---|
| Notice | No insight | Interesting fact | Changes the reader's view |
| Thesis | Summary only | Arguable but broad | Specific, meaningful, supportable |
| Evidence | Thin/unverified | Adequate | Verified, comparative, robust |
| Explanation | Jargon or formula dump | Understandable | Baseball reason and mechanism are clear |
| Integrity | Overclaiming | Caveats present | Scope, alternatives, and limits are explicit |
| Example | None or hypothetical | Relevant example | Complete verified case that teaches the method |
| Reader value | Unclear | Informative | Discovery, clarity, or anticipation is unmistakable |
| Watch | Generic ending | Practical takeaway | Reusable lens or genuine future test |

### Publication gate

- Minimum score: **13 of 16**.
- Thesis, Evidence, and Integrity must each score **2**.
- No dimension may score 0.

An article that fails the gate returns to the brief or research stage. Better prose does
not repair a weak idea.

---

## 14. MalliScore Article Brief Example

### Working title

**Building MalliScore: A Modern Way to Evaluate a Pitching Performance**

### Reader question

How can we evaluate the complete quality of one pitching performance without relying
only on its final runs allowed?

### Notice

Two outings with similar traditional pitching lines can represent meaningfully different
levels of dominance, run prevention, and completed work.

### Thesis

> **The conventional view says a pitching line or Game Score can summarize the quality
> of a start. That misses how the pitcher controlled the outing and how one weak
> dimension can change the whole performance. The evidence shows that combining
> dominance, run prevention, and workload creates a more reliable descriptive second
> opinion. This matters because fans can distinguish how a start was built, not only
> what the final line looked like.**

### Reader value

- Primary: Clarity.
- Secondary: Discovery.

### Scope

MalliScore describes the quality of one starting-pitcher outing.

### Boundary

It does not estimate pitcher talent or predict the next start. The validation found no
incremental next-start signal beyond recent form, and that null result must remain in the
article.

### Essential evidence

- The purpose and formula of the dominance, run-prevention, and workload components.
- A real worked outing from inputs to final score.
- Comparison with Bill James Game Score and credible disagreement cases.
- The 2024-2026 reliability study and sensitivity analysis.
- The V3 WHIP tail defect and V4 Reach Rate Allowed correction.
- Known limitations, including the score's relationship with outing length.

### Watch

When MalliScore and the traditional line disagree, inspect which pillar created the
difference and whether that changes the story of the outing.

---

## 15. Final Publication Checklist

Before publishing, verify:

- The Notice can be stated in one sentence.
- The thesis passes X / Y / Z / W.
- The primary reader value is clear.
- The opening begins with baseball, tension, or a meaningful question.
- Every important number is verified against its source.
- Data, reporting, inference, and unknowns are distinguishable.
- The conventional view is represented fairly.
- A real example follows every important formula or abstraction.
- The strongest reasonable alternative or counterexample is addressed.
- The article states what it does not prove.
- Every visual adds evidence and is mobile-readable.
- The ending provides a real Watch, not a generic CTA.
- The article passes the 13/16 readiness gate.
- The title promises the value the article actually delivers.

---

## 16. Research Basis

This system adapts recurring strengths from established analytical writing:

- FanGraphs' [Stuff+, Location+, and Pitching+ Primer](https://library.fangraphs.com/pitching/stuff-location-and-pitching-primer/): purpose, definition, scale, reliability, and practical use.
- FanGraphs' [THE BAT X and THE BATcast introduction](https://blogs.fangraphs.com/introducing-the-bat-x-for-pitchers-and-the-batcast-stuff-model/): origin, differentiation, question-driven explanation, and explicit model limitations.
- Baseball Prospectus' [DRA analysis](https://legacy.baseballprospectus.com/article_legacy.php?articleid=29898): asking what the metric actually tells the reader, comparing alternatives, and connecting reliability to use.
- FanGraphs' [complete pitcher WAR example](https://library.fangraphs.com/calculating-pitcher-war-a-complete-example/): teaching a complex method through one complete calculation.
- [X Articles guidance](https://help.x.com/en/using-x/articles): clear purpose, strong hook, compact paragraphs, descriptive subheadings, evidence, visuals, and a deliberate close.

These are structural references, not templates to imitate. Mallitalytics' identity,
NOTICE -> EXPLAIN -> WATCH system, integrity rules, and baseball voice remain the final
authority.
