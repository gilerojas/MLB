# Architecture Patterns

**Project:** Mallitalytics Showcase  
**Domain:** Public baseball editorial showcase with a private one-author publishing workflow  
**Researched:** 2026-08-10  
**Confidence:** MEDIUM — the current site and MLB Ops integration surfaces were inspected directly; platform guidance is current official Cloudflare documentation. The future ingest contract remains a design recommendation that needs implementation-phase validation.

## Recommended Architecture

Use `mallitalytics-public/` as an independently deployable Cloudflare Worker application with five deliberately separate concerns:

```text
 Public visitor                  Private author                     MLB Ops (future)
 ──────────────                 ───────────────                   ────────────────
 Browser → public routes         Browser → /admin routes            approved artifact + metadata
              │                           │                                  │
              ▼                           ▼                                  ▼
       read-only query layer       authenticated editorial API       signed ingest endpoint
              │                           │                                  │
              └──────────────┬────────────┴──────────────┬───────────────────┘
                             ▼                           ▼
                   D1 editorial metadata          R2 media objects
                   (structured, queryable)        (originals + derivatives)
                             │                           │
                             └──── published projection ─┘
                                      │
                                      ▼
                              public page/render cache
```

The public site must be anonymous, read-only, and unable to reach draft records or private assets. The private author workflow is the only normal path that can change an editorial item's publication state. R2 is the binary-object store, never the source of navigational truth; D1 owns the metadata, URLs, captions, taxonomy, revisions, and publication status. The existing file-backed Markdown and card manifest should stay live for the launch collection while the D1/R2 path is introduced, then be migrated item by item instead of through a large content rewrite.

Cloudflare Worker bindings are the correct integration boundary: bind D1 and R2 explicitly to this Worker, pass the environment/bindings into repository services, and do not expose cloud credentials to the browser. Use Worker secrets for the admin session secret and the later MLB Ops ingest secret, not repository configuration or plaintext Worker variables.

### Component Boundaries

| Component | Responsibility | Communicates With | Must Not Do |
|---|---|---|---|
| Public reader routes | Home, archives, type/date/player/team/topic filters, item detail pages, SEO/OG responses | `PublishedContentRepository`, public R2/CDN URLs | Authenticate writers, mutate content, list buckets, expose drafts |
| Presentation components | Render editorial layouts from a normalized published-item view model | Public reader routes | Parse arbitrary source files or decide publication status |
| Editorial admin routes | One-author dashboard, create/edit, media selection, preview, review checklist, publish/archive actions | Auth guard, editorial service, D1, R2 upload service | Serve public assets by bypassing published-state checks |
| Auth and authorization guard | Verify short-lived HTTP-only owner session; protect every `/admin` and write route; enforce CSRF/origin checks on mutations | Admin routes, audit log | Be reused as public-reader identity or trust a client-side role flag |
| Editorial service / repository | Validate input, build slugs, transact state changes, resolve taxonomy, issue publication projection | D1; R2 metadata service | Accept untrusted MLB Ops data as published content |
| D1 editorial database | Canonical structured metadata, relationships, revisions, audit entries, ingest candidates | Repository only | Store images, card binaries, or warehouse data |
| R2 media archive | Durable originals, public derivatives, thumbnails, optional generated previews | Admin upload service, public CDN | Be browsed/listed by visitors or treated as a CMS index |
| Ingest adapter (future) | Authenticate MLB Ops, validate a constrained manifest, write/update a `pending_review` candidate and asset references | D1/R2 service, audit log | Publish, replace a public URL, or use the public routes as an API |
| MLB Ops exporter (future) | Copy selected render output to a staging location and submit normalized metadata | Ingest adapter | Gain D1/R2 credentials or decide editorial approval |

### Data Model Concepts

Use a common `content_items` record for every public work type: `article`, `visual_card`, `visual_study`, `project`, `tracker`, `dashboard_snapshot`, and selected `social_post`. A common record gives the archive one query model while type-specific payloads remain in a versioned JSON field or satellite tables.

| Model | Essential fields / relationships | Why it exists |
|---|---|---|
| `content_items` | `id`, stable `slug`, `type`, `title`, `dek`, `body_markdown`, `status`, `published_at`, `featured_rank`, `canonical_url`, `source_kind`, timestamps | Canonical editorial unit and public routing target |
| `content_revisions` | item ID, immutable body/metadata snapshot, `revision_no`, author, change note, created timestamp | Preview and rollback without silently mutating history |
| `assets` | ID, `r2_key`, `visibility`, MIME type, byte size, checksum, width/height, alt text, credit/license, created timestamp | Makes media durable and auditable rather than scattered files |
| `content_assets` | item ID, asset ID, role (`hero`, `inline`, `gallery`, `card_full`, `thumbnail`, `og`), sort order, caption | Allows one original and several derivatives to be placed safely |
| `taxonomies` / joins | normalized `players`, `teams`, `topics`, `series`; content-to-taxonomy joins | Powers relevant archives without fragile string parsing |
| `publication_events` | item ID, from/to status, actor, reason, timestamp | Records review and publishing decisions |
| `ingest_candidates` | external ID, producer/version, source manifest checksum, raw manifest JSON, validation result, candidate item ID, received timestamp | Quarantines automation inputs and supports idempotent retries |
| `ingest_events` | candidate ID, action, outcome/error code, actor/service, timestamp | Operational audit trail independent of publication history |

Keep `body_markdown` and validated frontmatter in D1 for authorable long form. Use an explicit content schema per type (for example `card_type`, `season`, `game_date`, `metric_notes`, `external_permalink`) rather than putting all metadata in unvalidated JSON. JSON is appropriate only for bounded type-specific display configuration that does not drive core archive filters.

### State and Access Boundaries

```text
draft → ready_for_review → approved → published → archived
                    ↑              │
                    └── needs_changes

MLB Ops: received → validation_failed | pending_review → (author action only)
```

- Only the authenticated author can move an item to `ready_for_review`, `approved`, `published`, or `archived`. For a one-author system, `approved` is a deliberate checklist/preview gate, not a second human role.
- Public queries use `WHERE status = 'published' AND published_at <= now`; route handlers should fetch by published slug, not fetch then hide draft state in the UI.
- Preview uses an authenticated route with an explicit revision ID and must set `noindex`, no public cache, and no shareable permanent URL.
- Every state transition records `publication_events`; publishing must atomically mark the item published and select its public asset derivatives.
- Admin and ingest mutations require server-side authentication, origin/CSRF protection, strict schema validation, rate limits, and audit logging. The admin client never receives D1/R2/S3 credentials.

### Media Handling

Use two object namespaces/buckets rather than persisting generated media in `public/`:

```text
private-originals/{asset-id}/source.{ext}       # author/ingest only
public-media/{asset-id}/full.{webp|png}
public-media/{asset-id}/thumb.webp
public-media/{asset-id}/og.webp
```

- Keep source PNGs, source files, and unapproved uploaded media private. Public records point only to reviewed derivatives under a dedicated public prefix or public bucket.
- Put production public media behind an R2 custom domain such as `media.mallitalytics.com`, not an `r2.dev` development URL. R2 custom domains allow CDN caching and access controls; the public bucket root cannot be treated as an archive listing API.
- For the first author-only release, upload through a protected Worker action that streams/validates small files and writes directly to R2. Add direct browser upload only when necessary: issue a short-lived, single-object presigned `PUT` with exact key, MIME type, size policy, and bucket CORS restricted to the admin origin. Treat the URL as a bearer credential and never place it in public metadata or logs.
- Generate responsive derivatives on upload/build, record dimensions/checksum in `assets`, require useful alt text before `approved`, and retain the original as immutable. Replace an image by creating a new asset/relation, not by overwriting a public R2 key that a cached page may retain.
- Version public object keys by asset ID or content hash. Configure long immutable cache lifetimes for versioned media; page metadata can change independently without cache poisoning.

### Data Flow

#### Manual author publishing (first release)

1. The author logs into `/admin`; a server-side session guard protects the page and write routes.
2. The author creates a draft and uploads/selects assets. The Worker validates request size, allowed MIME/extension, dimensions and required alt/credit metadata, then stores private original and generated/public derivative records in R2/D1.
3. The editorial service writes a revision and a `draft` item transactionally. Preview reads that explicit revision only through the authenticated preview route.
4. The author marks it `ready_for_review`; the admin checklist verifies title/dek, valid slug, body rendering, taxonomy, hero/OG media, alt text, outbound links, and intended publish date.
5. `approved` and `published` are explicit server-side transitions. Publishing writes an audit event, makes the record eligible for public queries, and triggers revalidation/cache purge only for the affected routes.
6. Public pages query the published projection and render media by its public custom-domain URL. A failed media derivative or a missing public asset blocks the publish transition rather than yielding a broken launch item.

#### MLB Ops-assisted ingest (later)

1. MLB Ops generates an output and a small, versioned manifest in its own runtime: external artifact ID, type, title/dek, dates, player/team/topic IDs, provenance, source-output checksum, and explicit candidate media paths.
2. A dedicated exporter copies only selected candidates to the showcase ingest staging path or calls a dedicated Worker endpoint using a scoped service credential. It never shares the showcase author session or permanent bucket/database credentials.
3. The ingest adapter authenticates the request, validates signature/schema/size/MIME/checksum/idempotency key, copies media into private showcase R2 storage, and saves the raw manifest plus validation outcome in `ingest_candidates`.
4. Success creates or updates a `pending_review` draft linked to its source provenance. Validation failure is terminal for that attempt, preserved in `ingest_events`, and optionally notifies the author; it must not partially publish or overwrite an existing public item.
5. The author reviews the candidate in the same admin UI, fixes editorial copy/taxonomy/assets as needed, and alone performs the `approved → published` transition.

### Error and Review Gates

| Gate | Reject / hold condition | Required outcome |
|---|---|---|
| Input validation | Invalid body/frontmatter, unsafe URL, bad slug, unknown content type | Return a field error; retain previous revision unchanged |
| Upload validation | MIME/signature mismatch, size/dimension limit, duplicate checksum policy violation | Do not create a publishable asset; clean up incomplete private object asynchronously |
| Review checklist | Missing summary, taxonomy, alt text, hero/OG asset, or publish date | Item cannot enter `approved` |
| Publish transaction | Non-published/missing derivative, duplicate slug, stale revision conflict | Roll back status change; record error/audit event |
| Ingest auth/schema | Bad credential/signature, unsupported manifest version, replayed ID with different checksum | Quarantine candidate, log an `ingest_event`, no public side effect |
| Ingest media | Missing source asset, checksum mismatch, unacceptable image type | Candidate remains `validation_failed`; no public R2 key is issued |
| Public read | Published record points to unavailable asset | Serve a safe fallback and alert author; never fall through to private origin |

## Safe Incremental Build Order

1. **Stabilize the public reading model.** Preserve existing file-backed articles and `content/cards/manifest.json`; introduce a single `ContentRepository` interface with a file-backed implementation. Build public archive/detail/filtered listing routes against that interface so visual work can continue independently.
2. **Define the editorial schema and migration discipline.** Activate D1 in the deployment configuration, add Drizzle schema/migrations for `content_items`, taxonomy, assets, revisions, and publication events, and build a D1 implementation of the repository. Do not create a new database path outside Drizzle/D1.
3. **Add R2 and migrate a curated launch collection.** Enable R2 binding and custom media domain, copy only selected existing images/cards, create asset metadata and D1 records, verify responsive/OG rendering and 404 behavior. Keep static files working during the transition; each migrated item routes through D1, not both sources.
4. **Build the private author workflow.** Add owner auth, admin CRUD, protected preview, upload handling, checklist, state transitions, and audit UI. Prove manual publication, unpublish/archive, and recovery before attaching MLB Ops.
5. **Cut public reads to D1-backed published content.** Retire build-time content only after all launch entries are migrated and route/SEO parity is tested. Keep a scripted exporter/importer for Markdown and the card manifest as a rollback/migration tool, not a second runtime source of truth.
6. **Introduce a narrow MLB Ops bridge.** First export one content type (for example pitching cards) as `pending_review` candidates. Add idempotency, signed requests/scoped service credential, provenance, and failure visibility. Automation remains incapable of publishing.
7. **Expand only after reliability proves out.** Add more output types and optional bulk ingest once the author can diagnose failures, review candidates quickly, and trust source/asset provenance.

**Why this order:** public discovery and metadata contracts come before the admin because they establish the content shape; D1/R2 must exist before manual publishing can be durable; manual review must be proven before the more dangerous cross-system bridge; automation should be a producer adapter rather than a new publishing system.

## Patterns to Follow

### Repository With a Published Projection

**What:** Route all public reads through a repository method that only returns normalized, published content; admin routes have separate draft/revision methods.

**When:** From launch, including the temporary file-backed store.

**Example:**

```typescript
type PublishedContent = {
  slug: string;
  type: "article" | "visual_card" | "project" | "tracker";
  title: string;
  publishedAt: string;
  hero: { url: string; alt: string } | null;
};

interface ContentRepository {
  listPublished(filters: ArchiveFilters): Promise<PublishedContent[]>;
  getPublishedBySlug(slug: string): Promise<PublishedContent | null>;
  getRevisionForAdmin(id: string, revision: number): Promise<EditorialRevision | null>;
}
```

**Why:** A public page cannot accidentally expose drafts simply because a component forgot to filter a list.

### Append-Only Revisions and Events

**What:** Every edit creates an immutable content revision and every state change creates an event. The published item points to the selected revision.

**When:** All author updates and every ingest retry.

**Why:** It makes review, rollback, provenance, and stale-edit conflict handling tractable in a one-author workflow without building an enterprise CMS.

### One-Way, Candidate-Only Automation

**What:** The bridge has write capability only to an ingest endpoint that creates/updates a review candidate by external ID and checksum.

**When:** After manual publishing is reliable.

**Why:** MLB Ops owns analytical generation; the showcase owns public editorial judgment. Keeping the bridge one-way avoids coupling the public site to the warehouse/VPS and prevents an ingest bug from publishing or deleting public work.

## Anti-Patterns to Avoid

### Treating the Repository as a Media Archive

**What:** Continue adding cards and full-resolution images under `public/` and use file paths as CMS metadata.

**Why bad:** Deploy size, asset provenance, cache invalidation, and migration complexity rise together; the archive cannot distinguish a public derivative from a source original.

**Instead:** Put binaries in R2 and references/semantics in D1. Use content-addressed/versioned keys.

### One Table for Files, Metadata, and Workflow State

**What:** Store every asset detail and all content behavior in an opaque JSON blob or R2 object metadata.

**Why bad:** Archive filters, access rules, revisions, and review invariants become application guesses rather than database constraints.

**Instead:** Normalize items, assets, joins, taxonomy, and events; reserve JSON for limited type-specific fields.

### Direct MLB Ops-to-Public Publishing

**What:** Give the VPS a public-site admin cookie, broad R2 credentials, or an endpoint that accepts `status: published`.

**Why bad:** It crosses the private production runtime into the public publishing boundary, defeats editorial review, and makes replay/error recovery unsafe.

**Instead:** Use a scoped, signed candidate-ingest contract whose server forcibly assigns `pending_review`.

### Two Permanent Sources of Truth

**What:** Keep file-backed content and D1 as coequal runtime stores indefinitely.

**Why bad:** Listings, slugs, and publish status drift; every feature needs two implementations.

**Instead:** Use the file store only behind the common repository during migration, establish D1 as the durable publishing source, and retain import/export scripts for recovery.

### Admin Protection in the Client Only

**What:** Hide controls in React based on a browser variable or rely on a secret route name.

**Why bad:** Direct requests still mutate content or retrieve drafts.

**Instead:** Enforce authorization and CSRF/origin checks in Worker route handlers before service/repository calls.

## Scalability Considerations

| Concern | At launch / 100 readers | At 10K readers | At 1M readers |
|---|---|---|---|
| Public reads | D1 published queries with indexed filters; Worker page caching | Cache archive/detail responses, paginate/cursor lists, optimize D1 indexes | Aggressive CDN cache and pre-rendered/static publication projection; keep D1 off the hot path where possible |
| Media | R2 public custom domain, responsive derivatives | Immutable versioned keys and cache policies; thumbnails everywhere | Dedicated image transformation/derivative pipeline; monitor egress/cache and hot assets |
| Editorial workflow | Single author, sequential edits | Optimistic revision checks and event audit remain sufficient | Add roles/workflow separation only if multi-author editorial operations truly require it |
| MLB Ops bridge | Manual import or one candidate type | Idempotent signed endpoint, retry dashboard, dead-letter-style failed candidates | Queue/batch ingestion, rate limits, observability, independently deployable ingest worker |
| Taxonomy/search | Indexed D1 filtering by type/date/player/team/topic | Cursor pagination and denormalized published search fields | Dedicated search index only after D1 filtering/search evidence shows a need |

## Migration Constraints and Verification

- The current site deploys with vinext/Cloudflare Worker configuration, but `.openai/hosting.json` declares `d1` and `r2` as `null`; binding activation, real resource provisioning, and environment-specific deployment configuration are a prerequisite, not an implementation detail.
- `db/schema.ts` is intentionally empty and `lib/content/articles.ts` embeds Markdown at build time; therefore no current runtime content write path exists. Add migrations before admin screens and do not pretend an empty D1 binding is a CMS.
- Existing articles support only flat frontmatter and cards use a JSON manifest. Write a deterministic importer that maps them into the new model, reports duplicate/invalid slugs, and preserves original filenames/checksums. Do not rely on automated heuristic taxonomy for the curated launch set.
- Retain the public visual system and route URLs while moving data source. Test old/new canonical URLs, sitemap, OpenGraph images, date formatting, filtering, accessibility alt text, and public attempts to fetch draft slugs.
- Test publishing/integration failures as first-class cases: interrupted upload, stale revision, missing derivative, duplicate ingest delivery, invalid signature, payload too large, asset checksum mismatch, and source artifact removal after ingest.
- The public Worker must never need live access to MLB Ops's VPS, warehouse, Google Drive, FastAPI, or control-plane database on visitor requests. The bridge is asynchronous and explicit.

## Sources

- [Cloudflare Workers bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/) — MEDIUM confidence via the research-plan Context7 route (official documentation fallback inspected; updated 2026-07-22).
- [Cloudflare D1 Worker Binding API](https://developers.cloudflare.com/d1/worker-api/) and [D1 database batch API](https://developers.cloudflare.com/d1/worker-api/d1-database/) — MEDIUM confidence via the research-plan Context7 route (official documentation fallback inspected; updated 2026).
- [Cloudflare R2 presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/) — MEDIUM confidence via the research-plan Context7 route (official documentation fallback inspected; updated 2026-04-24).
- [Cloudflare R2 public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/) — MEDIUM confidence via the research-plan Context7 route (official documentation fallback inspected; updated 2026-06-16).
- [Cloudflare Workers secrets](https://developers.cloudflare.com/workers/configuration/secrets/) — MEDIUM confidence via the research-plan Context7 route (official documentation fallback inspected; updated 2026-07-03).
- Existing project evidence: `.planning/PROJECT.md`, `.planning/codebase/ARCHITECTURE.md`, `.planning/codebase/INTEGRATIONS.md`, and `mallitalytics-public/` source inspection — HIGH confidence for current-state observations.
