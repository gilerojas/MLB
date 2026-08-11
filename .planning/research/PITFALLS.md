# Domain Pitfalls: Mallitalytics Showcase

**Domain:** Brand-led baseball editorial publication and curated analytics showcase  
**Researched:** 2026-08-10  
**Overall confidence:** HIGH for platform and repository-specific risks; MEDIUM for workflow/modeling recommendations.

## Critical Pitfalls

### 1. Treating the historical output directory as the public archive

**What goes wrong:** A bulk import turns every generated card, test render, duplicate, partial, and out-of-context daily board into a public record. The archive becomes noisy, hard to browse, and makes the brand look less intentional than the prototype.

**Why it happens:** The current `sync-cards` script discovers files in the shared `outputs/` directory and derives much of its metadata from filenames. That is useful for a prototype, but output existence is not an editorial approval signal. The current manifest already exposes examples whose filenames describe fit checks and standard-production variants.

**Consequences:** Weak content obscures strong work; visitors encounter duplicates or missing context; future catalog cleanup becomes a destructive migration; storage and build sizes grow with no editorial gain.

**Warning signs:** “Import all” is the launch plan; a record has only inferred title/date/type; a test or `_fit_check_` asset appears in a public listing; archive pages are primarily file-name-shaped.

**Prevention:** Launch with an explicitly selected collection. Create an import inventory with a disposition for each source asset (`publish`, `hold`, `reject`, `needs metadata`), preserve provenance, and require title, summary/caption, alt text, content type, and an approved asset before publication. Import in batches by content family, not by recursive filesystem scan.

**Recommended phase mapping:** **Phase 1 — Archive foundation and curated migration.** Define the selection rubric and catalog importer before public discovery routes.

### 2. Using one loose model for articles, cards, projects, dashboards, and social work

**What goes wrong:** A generic `posts` table and free-text tag array initially feels fast, then cannot express recurring cards, article revisions, embedded projects, multiple media roles, player/team relations, or an archive facet reliably.

**Why it happens:** The existing prototype combines Markdown front matter and a card manifest. It is tempting to copy both into one unvalidated JSON shape.

**Consequences:** Filters become inaccurate, detail templates gain exception branches, data is duplicated across feature pages, and the private studio becomes more complicated than the public site.

**Warning signs:** Type-specific fields are optional blobs; a title, slug, image, or publish date is stored in more than one place; filtering uses substring matching; tags are added ad hoc with different spellings; an asset is treated as both a content item and a file.

**Prevention:** Model a canonical `content_item` with stable ID, immutable public slug policy, type, lifecycle state, publication timestamp, editorial copy, and provenance. Add bounded type-specific fields/revisions plus relational joins for people, teams, and topics. Model `asset` separately with object key, checksum, dimensions, derivative relationship, alt text, attribution/rights, and usage role (`hero`, `inline`, `card`, `social`). Maintain a small controlled taxonomy; tags are supplementary, never the sole discovery system.

**Recommended phase mapping:** **Phase 1 — Content model and migration.** Write validation rules and seed taxonomy before designing all archive filters.

### 3. Making generated files or public media URLs mutable

**What goes wrong:** A regenerated `latest.webp`, a replaced object at the same path, or a media URL coupled to the deployment filesystem changes a previously published card without an editorial revision. CDN caching then serves different versions unpredictably.

**Why it happens:** The current public site places derived WebP files under `public/cards/`, while MLB Ops serves generated PNGs from a worker-local output directory. Both are convenient development origins but not a durable public archive.

**Consequences:** Broken social embeds, stale thumbnails, irreproducible historical work, cache purges for routine corrections, and loss of a trustworthy record of analytical claims.

**Warning signs:** Object keys contain `latest`, `current`, or only a player/date; edits overwrite existing assets; D1 records filesystem paths or a bare URL but no checksum; source PNGs, thumbnails, and public derivatives cannot be traced together.

**Prevention:** Store source and approved derivatives in R2 under immutable, content-addressed or revisioned keys (for example `cards/2026/05/<content-slug>-<sha>.webp`). Keep object metadata and checksums in the catalog. Use a controlled public custom media domain only for approved derivative assets; preserve old keys indefinitely or redirect through an application-owned stable content URL. Repoint a content revision rather than replacing a binary.

**Recommended phase mapping:** **Phase 2 — Durable media and public delivery.** Migrate only curated media, create manifest/checksum validation, then switch runtime reads.

### 4. Accidentally exposing private media or operational systems

**What goes wrong:** Originals, working files, source data, previews, or direct R2 access become public alongside approved artwork; or the public site starts calling MLB Ops/VPS endpoints because that is where the latest files live.

**Why it happens:** A single bucket, a development `r2.dev` URL, browser-held write credentials, and direct filesystem/API integration collapse separate trust boundaries.

**Consequences:** Leakage of unpublished work or operational paths; a much larger attack surface; public reliability tied to warehouse availability; expensive recovery if broadly cached assets need to be removed.

**Warning signs:** `r2.dev` is used in production; the bucket has public access and private originals use predictable keys; the browser receives R2 credentials or a broad presigning capability; public routes call the MLB Ops API, VPS, warehouse, or Drive.

**Prevention:** Keep the bucket private by default; expose approved derivatives only through `media.mallitalytics.com` (not `r2.dev`). Cloudflare documents `r2.dev` as non-production/rate-limited and notes that custom domains are required for cache and access controls. Disable `r2.dev` when a bucket is protected behind custom-domain controls. Separate private originals from public derivatives by prefix/bucket and authorization policy. Protect `/studio` and studio mutations with Cloudflare Access; validate short-lived, scoped upload requests server-side. The public app reads only its D1 catalog and approved media domain.

**Recommended phase mapping:** **Phase 2 — Security and media delivery** before any author upload or external import endpoint.

### 5. Letting successful automation equal publication

**What goes wrong:** A completed MLB Ops card, analysis job, or AI drafting run becomes public automatically, even when data was incomplete, the copy is inaccurate, visual output failed a quality check, or it simply does not suit the showcase.

**Why it happens:** MLB Ops presently has broad orchestration modules, fragile external-service boundaries, limited provider contract tests, request/script-coupled job lifecycles, and data freshness risks. Those failure modes are manageable inside an operator tool but unsafe as a public-publishing trigger.

**Consequences:** Incorrect analysis is published with the brand’s authority; accidental duplicates and re-publishes; a generator outage breaks the public site; emergency removal needs manual data surgery.

**Warning signs:** An upstream job may write `published`; an import payload contains public URL/status fields; no human can preview a candidate in its final template; candidates lack source dataset/run/version/checksum; publication and generation metrics are conflated.

**Prevention:** Treat automation as a candidate feed. A narrow authenticated adapter may create `candidate`/`draft` records and attach immutable assets, but can never set `published`, overwrite curated copy, or access D1 directly. Capture provenance (generator, source date/window, run ID, data freshness, source asset checksum); validate schemas; deduplicate idempotently; require preview plus explicit author approval; retain reject/archive reasons. The public query must enforce `state = published AND published_at <= now` server-side.

**Recommended phase mapping:** **Phase 3 — Manual studio workflow.** **Phase 4 (later) — Review-gated MLB Ops candidate ingestion**, after manual publishing has proven the model.

### 6. Skipping revision, unpublish, and redirect semantics

**What goes wrong:** Correcting a published chart overwrites history; unpublishing deletes the only record; changed slugs break inbound links; scheduled content leaks early due to client-side filtering.

**Why it happens:** A single `published` Boolean omits lifecycle and immutable revision concepts, while static-site habits make deletion feel normal.

**Consequences:** Loss of provenance, broken URLs and social links, unrecoverable editorial mistakes, and a difficult transition from Markdown/Git to a live catalog.

**Warning signs:** A content row has no revision/version; `DELETE` is the normal unpublish operation; one URL field is overwritten for every title change; preview data is loaded by public pages and hidden only in the UI.

**Prevention:** Use explicit states (`draft`, `review`, `published`, `archived`, `rejected`) and a revision record or immutable snapshot per publication. Make unpublish/archive reversible, record who/when/why, preserve public slugs, and maintain permanent redirects for intentional route changes. Enforce visibility in Worker/D1 queries, not only in React. Apply D1 migrations as ordered, version-controlled files and target the immutable production database name; Cloudflare records applied migrations and warns that local defaults can otherwise be mistaken for production.

**Recommended phase mapping:** **Phase 1 — Catalog lifecycle/migrations**, with publish/unpublish test coverage completed in **Phase 3 — Studio**.

### 7. Building a full SaaS while trying to make a focused publication

**What goes wrong:** The roadmap adds reader accounts, comments, subscriptions, social scheduling, a generic CMS, live scores, full dashboards, or team collaboration before a visitor can browse a strong curated archive.

**Why it happens:** MLB Ops already contains powerful internal workflows, and feature-complete media tools make their workflows appear necessary. But the project’s product is a brand-led showcase, not a consumer platform or a replacement MLB Ops control plane.

**Consequences:** Diluted visual voice; auth, moderation, payment, compliance, and support work; a needlessly complex schema; launch delayed without improving discovery or editorial craft.

**Warning signs:** Requirements mention readers managing data, creating accounts, receiving live alerts, or paying before the manual studio has shipped; scope introduces multiple user roles; archive work waits for dashboard/API integration.

**Prevention:** Keep v1 to anonymous public reading plus one private author workspace. Measure success through coherent collection quality, navigation, publishing time, asset durability, and content accuracy—not engagement mechanics. Put monetization, accounts, comments, live data products, and direct social publishing on an explicit deferred list with re-entry criteria.

**Recommended phase mapping:** **All phases — scope guard.** Review the out-of-scope list at each phase gate.

## Moderate Pitfalls

### 8. Letting taxonomy promise discovery the metadata cannot support

**What goes wrong:** Archive controls advertise player, team, topic, date, and content-type browsing, but imported records lack reliable structured associations. Search/filter pages silently omit important work.

**Prevention:** Only launch a facet after its coverage threshold is met and report “unknown/not tagged” in the private audit, not in public navigation. During curation, map canonical MLB IDs/names and current/historical team context separately; distinguish a featured player from every player mentioned in an article.

**Recommended phase mapping:** **Phase 1 — Import audit**, then **Phase 3 — Archive discovery** only for proven facets.

### 9. Treating alt text, attribution, and source context as polish

**What goes wrong:** Cards are visually attractive but inaccessible and analytically opaque; copied or third-party visuals are used without enough provenance; a chart has no date/window or methodology link.

**Prevention:** Make `alt_text`, author/rights status, original source reference, data-through date, and methodology/context links publish requirements where applicable. Add accessible long-form explanation beside dense data visuals rather than stuffing all information into alt text.

**Recommended phase mapping:** **Phase 1 — Content contract** and **Phase 3 — Studio validation.**

### 10. Coupling public availability to live baseball data or the production deploy

**What goes wrong:** The public archive becomes slow or blank because a season roll-over, stale warehouse partition, failed ingest, or server-local generated asset interrupts the application’s request path.

**Prevention:** Publish immutable editorial snapshots, not live warehouse queries. Content may cite a data-through timestamp, but public pages should retrieve catalog metadata and media independently of MLB Ops. Treat live scores as a separately bounded, optional enhancement with cache/failure UI—not a dependency of archive pages.

**Recommended phase mapping:** **Phase 2 — Runtime boundary**; defer any live-data enhancement to a separately researched phase.

## Phase-Specific Warnings

| Phase topic | Likely pitfall | Mitigation |
|---|---|---|
| Curated launch migration | Importing every historical artifact to look complete | Selection inventory, quality rubric, disposition, and batch imports by family |
| Content schema | Generic posts + tag blobs or a filesystem manifest as truth | Typed canonical content items, separate assets/revisions, controlled relational taxonomy |
| R2 migration | Mutable keys, public originals, `r2.dev`, and lost source/derivative relations | Immutable versioned keys, custom media domain, private originals, checksums and asset manifest |
| D1 rollout | Preview/local schema differs from production | Ordered migrations, migration ledger, immutable production DB name, deploy check |
| Studio | “Draft” can be fetched publicly or publication is a Boolean | Server-enforced state machine, preview, explicit publish/unpublish, audit fields |
| MLB Ops connection | Data/generation success directly changes public state | Candidate-only signed adapter, provenance, idempotency, required author approval |
| Archive filters | Facets shipped before metadata coverage exists | Import completeness audit and phased discoverability |
| Product direction | Scope expands toward a multi-user SaaS or internal dashboard | Keep v1 anonymous/public plus one protected author workflow; phase-gate deferred products |

## Sources

- [Cloudflare R2 public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/) — **HIGH**: private-by-default behavior, custom-domain capabilities, and `r2.dev` limitations.
- [Cloudflare R2 limits](https://developers.cloudflare.com/r2/platform/limits/) and [how R2 works](https://developers.cloudflare.com/r2/how-r2-works/) — **HIGH**: production delivery and caching guidance.
- [Cloudflare D1 migrations](https://developers.cloudflare.com/d1/reference/migrations/) — **HIGH**: ordered migration tracking and target-database caveats.
- [Codebase Concerns](../codebase/CONCERNS.md) — **HIGH**: repository-specific evidence on generated asset hosting, ingest fragility, migration risk, and automation boundaries.
- [Content modeling best practices](https://headlesscms.guide/guides/content-modeling-best-practices) and editorial workflow research — **MEDIUM**: taxonomy, metadata, and review-gate guidance; recommendations are tailored to the project rather than treated as platform facts.

## Research Notes

- The research-cache seam stored the workflow/modeling digests. Its cache directory did not permit writing the two official-source fallback digests; their linked Cloudflare documentation was nevertheless read and cited directly.
- What might be missing: final asset licensing/rights requirements and the exact desired archival retention policy should be resolved while choosing the launch collection. Neither changes the need for provenance fields and non-destructive lifecycle states.
