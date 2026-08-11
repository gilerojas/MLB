# Project Research Summary

**Project:** Mallitalytics Showcase  
**Domain:** Brand-led, data-driven baseball editorial showcase and living archive  
**Researched:** 2026-08-10  
**Confidence:** MEDIUM

## Executive Summary

Mallitalytics should launch as a public, English-language editorial showcase: a deliberately curated collection of articles, pitching cards, visual studies, projects, trackers, and selected social work. It is neither a generic portfolio nor a live-statistics product. The public experience should make each work's baseball claim, visual evidence, context, and methodology legible to fans while proving analytical craft to industry readers. The launch collection must be selected and fully described; importing every historical MLB Ops artifact would weaken the brand and discovery experience.

Build on the existing React/Vinext visual prototype and Cloudflare Worker deployment rather than rewriting it. Use a static-first public app with Cloudflare D1 as the single editorial catalog and R2 as the durable media archive, accessed through a production custom media domain. A narrow, Access-protected one-author studio must support drafting, previewing, editing, publishing, unpublishing, and asset selection without code edits. The architecture should keep MLB Ops private: it may later create review candidates through a signed boundary, but it can never publish directly.

The principal launch risk is crossing editorial and operational boundaries too early: mutable local files, loose metadata, public drafts, broad credentials, or auto-publication would undermine both reliability and trust. Mitigate this with an explicit content lifecycle, append-only revisions and publication events, immutable media keys/checksums, server-enforced public queries, controlled taxonomy, and review gates. Preserve existing public URLs and visual direction while migrating a curated subset incrementally.

## Key Findings

### Recommended Stack

Keep the current TypeScript/React 19, Vinext/App Router, Cloudflare Worker, Tailwind 4, and existing CSS/GSAP implementation for the first release. The stack choice is intentionally conservative: the public application needs routes, static rendering, Markdown display, and narrow route handlers—not advanced Next features that Vinext may not yet support. Treat a standard Next.js/OpenNext move as a future escape hatch if preview deployments demonstrate a Vinext compatibility blocker, not as launch work.

**Core technologies:**

- **Cloudflare Workers + Vite plugin:** public routes, static assets, and protected studio endpoints — retains the deployed prototype and provides a simple edge boundary.
- **Cloudflare D1 + Drizzle:** canonical editorial records, relationships, revisions, lifecycle state, and indexed discovery — appropriate for a low-write, one-author catalog; use committed, ordered migrations against the immutable production database identifier.
- **Cloudflare R2:** immutable originals and publishable derivatives — keep object binaries out of Git and serve approved derivatives through a custom domain, never `r2.dev`.
- **Cloudflare Access:** one-author protection for `/studio` (or `/admin`) and write routes — avoids building a reader-account/authentication product.
- **Markdown + validated front matter:** portable article source and revision content — preserve original Git sources during migration; render only sanitized, allowlisted HTML.
- **Zod:** validation for every studio mutation, imported front matter, and future candidate manifest.

**Service-choice resolution:** D1/Drizzle and a purpose-built studio are the right launch choice over Postgres/Supabase/Neon or a hosted CMS because collaboration, high write volume, and SaaS workflow are explicitly absent. Use an Access allowlist rather than custom application credentials. Use Worker-mediated small uploads first; add presigned R2 browser uploads only when upload volume or size makes it necessary, with short expiry, exact object scope, MIME/size checks, and restrictive CORS.

### Expected Features

**Must have (V1 table stakes):**

- Curated editorial home with a clear lead piece, recent work, selected visual work, and intentional archive paths.
- Stable detail pages for every published work type, including title, summary/dek, date, byline, visual/body or embed, method/source context where applicable, and related/next paths.
- Small, editorially meaningful content-type archives plus only those topic/player/team/date facets supported by complete metadata.
- Basic retrieval through metadata/title search or a graceful archive-finder fallback.
- A private manual workflow: draft, revision-aware preview, metadata editing, explicit publish, unpublish/archive, and media selection/upload.
- Responsive, accessible visual reading: legible mobile cards/charts, enlargement or a dedicated visual view, alt text, descriptive captions, canonical URLs, and Open Graph metadata.
- Brand-led About, methodology/corrections, contact, and non-betting editorial-position surfaces.

**Should have (first-release differentiators):**

- A repeatable claim → evidence → context → method → caveat content template.
- A visual research gallery whose cards have individual explanations, sources/data windows, and related analysis rather than functioning as uncaptioned images.
- Brief “work behind the work” method notes that demonstrate craft without exposing private operational systems.

**Defer (after V1 proves the editorial model):** curated cross-format collection pages, recurring tracker update histories, metadata-driven related-work recommendations, and the MLB Ops candidate review queue. Keep subscriptions, payments, reader accounts/comments, betting products, live scores, generic dashboard replacement, blind historical imports, and automated publication out of this milestone.

### Architecture Approach

The public application and author workflow share content contracts but not permissions. Public routes read only a normalized published projection from a `ContentRepository`; private routes access revisions and lifecycle transitions through a server-side editorial service. D1 is the sole navigational and publication truth, while R2 is an asset store with private originals and approved public derivatives. Existing Markdown and card manifest files remain a temporary, importer-backed launch source—not a second permanent runtime truth.

**Major components:**

1. **Public reader routes and presentation layer** — home, archive/filter, detail, SEO, and OG rendering from published-only view models.
2. **Editorial service and repository** — validates content, resolves taxonomy, manages stable slugs/revisions, and atomically changes lifecycle state.
3. **D1 editorial catalog** — `content_items`, revisions, assets, relations, controlled taxonomy, publication events, and later ingest candidates.
4. **R2 media archive** — immutable private originals plus approved, versioned derivatives served from `media.mallitalytics.com`.
5. **Access-protected studio** — author-only creation, upload/selection, preview, review checklist, publish/unpublish, and audit visibility.
6. **Future one-way MLB Ops ingest adapter** — validates and quarantines signed candidate manifests; it cannot set `published`, overwrite public content, or access D1/R2 directly.

**Key patterns:** published-projection repository for every public read; append-only revisions plus publication events; explicit `draft → ready_for_review → approved → published → archived` transitions; server-side visibility predicate (`status = published` and scheduled time reached); immutable versioned media; and an asynchronous candidate-only integration boundary.

### Critical Pitfalls

1. **Importing a historical dump instead of curating a launch collection** — create an asset/content inventory with `publish`, `hold`, `reject`, and `needs metadata` dispositions; migrate by content family only after every selected item has required editorial context.
2. **Using an unstructured “posts plus tags” model** — separate canonical items, revisions, assets, asset roles, and normalized player/team/topic relations; use bounded type-specific fields rather than an opaque JSON blob or filename-derived archive.
3. **Overwriting generated assets or coupling them to the deploy filesystem** — store immutable content-addressed/revisioned R2 keys, checksums, source/derivative relations, and make corrections new revisions rather than replacements.
4. **Exposing private media, drafts, or MLB Ops infrastructure** — private-by-default originals, public derivative custom domain, edge-protected studio, scoped upload capability, and no visitor-time calls to VPS, warehouse, FastAPI, or Drive.
5. **Allowing generation success to equal public publication** — enforce candidate-only ingest, schema/signature/checksum/idempotency checks, provenance, preview, and explicit author approval; public data access must remain independent of live baseball data.

## Implications for Roadmap

### Phase 1: Content Contract, Curated Inventory, and Public Reading Foundation

**Rationale:** The catalog and launch selection determine what can be credibly browsed, how assets migrate, and which archive facets are honest. Establish this before building a CMS or moving all media.

**Delivers:** a selection rubric and import inventory; controlled launch taxonomy; stable content types and metadata contract; a `ContentRepository` abstraction with temporary file-backed implementation; public home, detail, and content-type/archive routes using normalized published views; preserved legacy slugs and a migration/redirect map.

**Addresses:** curated home, durable detail pages, content-type archives, metadata/search foundation, visual-work context, methodology conventions.

**Avoids:** historical-dump launch, loose post/tag model, duplicate runtime sources, and taxonomy promises not supported by data.

### Phase 2: Durable Editorial Catalog and Media Migration

**Rationale:** Real manual publishing and reliable public discovery depend on an authoritative D1 model and durable R2 assets. Move a curated subset only after the contract and migration checks exist.

**Delivers:** provisioned Worker bindings; Drizzle/D1 schema and migration discipline; items, revisions, assets, taxonomy joins, and publication events; an idempotent Markdown/card-manifest importer; R2 private/public asset namespaces, immutable keys/checksums, responsive derivatives, and custom media domain; D1-backed published reads for migrated work.

**Uses:** Cloudflare Workers, D1, Drizzle, R2, versioned SQL migrations, and a production custom domain.

**Avoids:** mutable local/deploy media, `r2.dev`, public originals/bucket listings, untraceable derivatives, and local/production D1 drift.

### Phase 3: Author Studio and Editorial Safety Gates

**Rationale:** Manual, reviewable publication is a V1 release blocker and must be reliable before automation is considered.

**Delivers:** Cloudflare Access-protected studio; authenticated server-side CRUD; upload/asset selection; revision-specific private previews; validation and review checklist; explicit publish/unpublish/archive; audit events; safe Markdown rendering; content and asset recovery/error paths.

**Addresses:** no-code author publishing, revision, correction, accessibility/alt text, source/method visibility, and dependable archive growth.

**Avoids:** client-only auth, Boolean-only publication state, draft leakage, destructive deletion, missing alt/rights/provenance, and accidental scheduled-content exposure.

### Phase 4: Launch Curation, Discovery Polish, and Production Validation

**Rationale:** The foundation should be proven through a cohesive public collection, not treated as finished when services merely exist. Release only discovery paths supported by the actual migrated metadata.

**Delivers:** selected launch collection fully migrated and editorially reviewed; responsive visual gallery and enlargement; basic search/archive finder; canonical/OG/sitemap parity; About/methodology/contact pages; performance, accessibility, broken-link/media, draft-boundary, and publishing/unpublish acceptance tests.

**Addresses:** full V1 visitor journey, sharing, mobile visual reading, brand-led authorship, and launch acceptance signals.

**Avoids:** empty filters, unexplained visuals, static-mockup feel, broken social links, and scope drift into accounts/payments/live data.

### Phase 5: Review-Gated MLB Ops Candidate Bridge (Later Milestone)

**Rationale:** Only add operational integration after manual workflows establish the exact required fields, review checks, asset conventions, and recovery practice.

**Delivers:** a narrow signed candidate endpoint/exporter for one content type (recommended: pitching cards); staging/private asset transfer; strict manifest validation, provenance, checksum/idempotency, audit/failure visibility, and author review in the same studio.

**Addresses:** the safe evolution path from MLB Ops-assisted publishing without changing the public site’s editorial ownership.

**Avoids:** direct database/bucket credentials on the VPS, public dependencies on warehouse/API freshness, accidental duplicate or inaccurate publication, and automation scope creep.

### Phase Ordering Rationale

- Curation and normalized content contracts precede services because they define the honest launch archive and prevent expensive schema/route rework.
- D1/R2 and immutable media must precede the studio because durable, auditable storage is required before manual publishing can replace code edits.
- A tested manual review workflow precedes every MLB Ops connection; editorial approval is the product boundary, not a later enhancement.
- Discovery polish comes after migration coverage is known, so navigation exposes only facets and collections with meaningful inventory.
- Automation is a later, independently bounded milestone—not a dependency of the public launch.

### Research Flags

Phases likely needing deeper research during planning:

- **Phase 2:** Verify existing Vinext deployment configuration, D1/R2 binding/provisioning, migration targeting, import edge cases, desired asset retention/licensing rules, and custom-domain setup in the actual Cloudflare account.
- **Phase 3:** Confirm Cloudflare Access identity-provider/allowlist configuration, owner-session/CSRF design, file-size/derivative processing limits, and Markdown sanitization implementation appropriate to the installed runtime.
- **Phase 5:** Research the precise MLB Ops candidate manifest, service authentication, transfer method, retry/dead-letter behavior, provenance fields, and operational ownership after V1 proves manual publication.

Phases with standard patterns (research can be light):

- **Phase 1:** Repository abstraction, stable routes, controlled taxonomy, and curated content audit are well-bounded by the project brief and existing source formats.
- **Phase 4:** Responsive editorial presentation, canonical/OG metadata, accessibility checks, and archive UX are established web-publication patterns; validate against the actual collection rather than researching a new platform.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | MEDIUM | Cloudflare Workers/D1/R2 facts were verified against official documentation; the D1-over-Postgres and Access configuration choices are strong launch-fit judgments, while Vinext remains an active-development risk. |
| Features | MEDIUM | V1 scope and priorities are strongly grounded in `PROJECT.md`; external publication examples are directional rather than decisive. |
| Architecture | MEDIUM | Current site/MLB Ops boundaries were inspected directly and Cloudflare service constraints are documented; the future ingest contract requires implementation validation. |
| Pitfalls | HIGH | Major risks are corroborated by project constraints, existing operational boundaries, and official Cloudflare guidance on media/migrations. |

**Overall confidence:** MEDIUM — enough for roadmap creation, with account configuration and the actual launch inventory requiring early validation.

### Gaps to Address

- **Launch inventory:** select the actual representative articles, cards, studies, projects, trackers, and social work; record per-item content type, native body/embed requirement, rights/provenance, alt text, and migration disposition before committing archive filters.
- **Taxonomy coverage:** validate player/team/topic metadata coverage and only enable public facets that meet the curated collection’s threshold.
- **Rights and retention:** decide asset licensing/credit requirements, whether private source originals have a formal retention policy, and how historical corrections are disclosed.
- **Cloudflare configuration:** provision and test production D1/R2 bindings, Access identity setup, custom media domain, secrets, migration procedure, and preview/production separation.
- **Vinext contingency:** test the required route, Worker, Markdown, D1/R2, and studio features in deploy previews; trigger a focused OpenNext/standard Next assessment only if a concrete compatibility limitation blocks them.
- **Search threshold:** begin with indexed metadata/title retrieval; introduce a dedicated search service only if catalog scale and observed D1 query behavior justify it.

## Sources

### Primary (HIGH confidence)

- [Project brief](../PROJECT.md) — scope, audience, constraints, and explicit out-of-scope decisions.
- [Stack research](STACK.md) — official Cloudflare documentation for Workers static assets, D1 migrations/bindings, R2 storage/public delivery, and Vinext project-status evidence.
- [Architecture research](ARCHITECTURE.md) — direct current-codebase observations and Cloudflare binding, R2, and secret guidance.
- [Pitfalls research](PITFALLS.md) — codebase-specific operational boundaries, D1 migration risks, and R2 production delivery controls.

### Secondary (MEDIUM confidence)

- [Features research](FEATURES.md) — editorial showcase expectations and prioritization; external examples are used as supporting context rather than new requirements.

---
*Research completed: 2026-08-10*  
*Ready for roadmap: yes*
