# Feature Landscape

**Domain:** One-author, data-driven baseball editorial showcase and living archive  
**Researched:** 2026-08-10  
**Overall confidence:** MEDIUM — v1 prioritization is grounded primarily in the project brief; ecosystem observations were cross-checked against current baseball and journalism examples but carry LOW provider confidence.

## Product Principle

Mallitalytics should feel like a publication with a point of view, not a portfolio grid and not a live-statistics terminal. A visitor must be able to enter through a strong piece of work, understand its baseball claim and method, then continue into a deliberately organized body of related work. The author retains the final publishing decision for every public item.

## Table Stakes

Features users expect. Missing = the site feels like a static mockup rather than a usable editorial home.

| Feature | Why Expected | Complexity | Release | Notes |
|---|---|---:|---|---|
| Curated editorial home | Visitors need immediate proof of what Mallitalytics publishes and where to begin. | Med | First release | Hero/lead story, recent work, selected visual work, and explicit paths into the archive. Do not imitate a generic chronological news feed. |
| Durable detail pages for every published item | Articles, cards, studies, projects, trackers, and selected social work need stable, shareable homes. | Med | First release | Each page needs title, deck/summary, date, byline, cover/visual, content body or embed, source/method note when relevant, and related work. |
| Content-type archive routes | A visitor expects to browse separately by writing, visual cards, studies/projects, trackers, and social selections. | Med | First release | Keep primary types small and editorially meaningful; avoid a mega-menu of MLB Ops output types. |
| Essential metadata and filtering | An archive needs more than reverse chronology to be rediscoverable. | Med | First release | Require content type, publication date, topics, and status. Add player and team only where genuinely relevant; topic pages must not be empty shells. |
| Search or a clear archive finder | The collection will become unusable as it grows without a direct retrieval route. | Med | First release | Start with title, deck, topic, player, and team search; filters can provide graceful fallback if full-text indexing is deferred. |
| Private, manual publishing workflow | The author must publish and revise without touching site code. | High | First release | Draft, preview, publish/unpublish, and edit metadata are the minimum. This is the safe foundation for any later MLB Ops integration. |
| Responsive, fast visual reading experience | Baseball cards and charts are core work, yet readers will arrive on phones and links. | Med | First release | Art direction must preserve legibility: offer tap-to-enlarge/lightbox or a dedicated visual view, descriptive alt text, and readable surrounding context. |
| About, methodology, and contact surfaces | Fans need orientation; industry readers need to understand authorship and analytical standards. | Low | First release | Brand-led voice, author secondary. Explain data/source conventions, corrections, and that analysis is not betting advice. |
| Sharing and canonical links | Showcase work needs to travel cleanly from social posts and direct referrals. | Low | First release | Human-readable URLs, Open Graph cards, canonical metadata, and copy-link/share controls. |

## Differentiators

Features that make Mallitalytics memorable rather than merely complete.

| Feature | Value Proposition | Complexity | Release | Notes |
|---|---|---:|---|---|
| Editorial-to-evidence story structure | Makes quantitative work readable to fans and credible to industry readers: a concise claim, the visual evidence, context, method, and caveats. | Med | First release | A reusable story/card template should make this a publishing standard, not a prose-only guideline. |
| Visual research gallery with narrative captions | Turns pitching cards and studies into a browsable visual archive, instead of orphaned image files. | Med | First release | Every visual gets its own title, explanation, source/date, and link to related analysis. Avoid an uncaptioned Pinterest-style wall. |
| “The work behind the work” method notes | Shows craft without becoming a résumé or exposing raw internal operations. | Low | First release | Brief, human-readable notes: data window, sources, definitions, limitations, and last updated date. Use selectively for analytical items. |
| Curated collection pages | Lets the author create meaningful editorial paths such as a pitcher study, season theme, or visual series across formats. | Med | Later evolution | Build after enough launch content exists; model a collection as a curated ordered list, not an automatic tag page. |
| Living tracker entries with update history | Demonstrates sustained analytical attention and makes recurring work useful beyond its original post date. | High | Later evolution | Each tracker needs a declared cadence, data freshness label, update log, and archival snapshot. Do not promise real-time data in v1. |
| Related-work graph | Helps a visitor move from a player, team, pitch type, or topic into the archive. | Med | Later evolution | Start with author-curated related links; consider metadata-driven suggestions only once taxonomy quality is proven. |
| Review queue for MLB Ops-assisted drafts | Preserves the route from operations work to publishing while keeping editorial judgment visible. | High | Later evolution | Ingest candidate material as a private draft with provenance and preview; never allow source systems to auto-publish. |

## Anti-Features

Features to explicitly not build in this milestone.

| Anti-Feature | Why Avoid | What to Do Instead |
|---|---|---|
| Subscription, paywall, payment, or premium funnel | Conflicts with the current visibility-and-craft objective and creates commerce work before editorial fit is proven. | Keep all launch work public; revisit only after audience and repeat-value signals exist. |
| Reader accounts, comments, forums, or social feed mechanics | Adds moderation, privacy, and retention obligations while distracting from a one-author archive. | Use share links and an external contact/social channel. |
| Betting picks, odds conversion, affiliate calls to action, or performance promises | Dilutes the publication’s independent analytical position and is explicitly out of scope. | State an editorial/methodology policy and focus on explanation, evidence, and uncertainty. |
| Live-score, real-time-stat, or full Baseball Savant/FanGraphs replacement | Expensive data freshness and product-surface commitments would overshadow curated editorial work. | Publish bounded studies and snapshot cards with clear data windows; link out to primary/reference sources where useful. |
| Blind import of the whole MLB Ops archive | Historical volume without selection weakens first impressions, taxonomy, and data quality. | Launch a handpicked collection with complete metadata; migrate in curated batches. |
| Unreviewed automated publishing | Risks inaccurate, context-free, or duplicative public work. | Allow automation only to create private drafts for author review, with source provenance. |
| Generic résumé/CV presentation | Makes the brand secondary and narrows the audience’s reading path. | Use a concise brand-led About page and let durable work demonstrate capability. |
| Empty filters, empty topic pages, or speculative sections | A sparse taxonomy feels unfinished and harms discovery. | Publish a route only after it has enough curated material; use broad, controlled topics at launch. |

## Feature Dependencies

```text
Content model + controlled taxonomy → detail pages → archive routes + search/filtering
Content model + asset storage → visual research gallery → visual detail pages + sharing cards
Manual author workflow → draft/preview → curated launch collection → reliable archive growth
Detail-page metadata + method notes → related work → future related-work graph
Curated launch collection + stable data/source conventions → living trackers and collection pages
MLB Ops source contract + private draft queue + human approval → assisted publishing (later only)
```

## MVP Recommendation

Prioritize:

1. **A curated editorial home and launch collection** — lead with a finite selection of articles, cards, studies, projects, trackers, and selected social work that has been edited for public presentation.
2. **A durable content model and detail-page system** — content type, title, date, summary, body/embed, visual asset, topic, optional player/team, sources/method, and publication status.
3. **Browseable archives and basic retrieval** — content-type routes plus small, controlled topic/player/team pathways and title/metadata search.
4. **A private manual publishing workflow** — draft, preview, edit metadata, publish, unpublish; this is a release blocker, not back-office polish.
5. **Visual work with context** — chart/card enlargement and explanatory captions/method notes so images become analytical artifacts rather than decoration.

Defer:

- **Curated collection pages:** wait until the launch archive reveals natural editorial series.
- **Living tracker update history:** add only after a tracker’s cadence and data-freshness contract are operationally sustainable.
- **Metadata-driven related-work recommendations:** begin with curated links and automate only after taxonomy cleanup.
- **MLB Ops-assisted publishing:** adopt a private draft/review queue after manual publishing has demonstrated the correct fields, review checks, and asset conventions.
- **All commercial, account, community, betting, and real-time-data features:** out of scope for the showcase milestone.

## Acceptance Signals for First Release

- A new visitor can reach a representative article, visual card, project, and tracker within two interactions from the home page.
- Every launch item has a stable URL, publication date, content type, summary, and usable visual/mobile presentation.
- An author can create, preview, correct, publish, and unpublish an item without editing deployed website code.
- A visitor can move from an item to at least one intentional archive route or related item.
- Analytical items state the relevant data/source window or link to a methodology convention; no page presents betting advice or unreviewed automation as editorial work.

## Sources

- [Mallitalytics Showcase project brief](/Users/gilrojasb/Desktop/Mallitalytics_VS/MLB/.planning/PROJECT.md) — HIGH confidence for product scope and constraints.
- [FanGraphs Lab announcement](https://blogs.fangraphs.com/introducing-the-fangraphs-lab/) — LOW confidence provider classification; supports keeping experimental visual/data work clearly separated and contextualized.
- [FanGraphs home page](https://www.fangraphs.com/) — LOW confidence provider classification; supports distinct editorial, data, and tool surfaces rather than a single undifferentiated feed.
- [Nieman Journalism Lab archive](https://www.niemanlab.org/archives/) — LOW confidence provider classification; supports subject navigation and archive search as complementary retrieval mechanisms.
- [RantSports editorial principles](https://www.rantsports.com/rankings/list/editorial-independence-publishing-principles/) — LOW confidence provider classification; supports human approval, corrections, and editorial independence.

### What might be missing

Launch-item inventory and the actual publishing CMS are still undecided. Before implementation, audit the source assets to validate which content types need native bodies, embeds, downloadable/exported artifacts, or update histories; use that audit to finalize metadata fields and the first archive taxonomy.
