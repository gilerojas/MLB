# Technology Stack

**Project:** Mallitalytics Showcase  
**Researched:** 2026-08-10  
**Recommendation confidence:** MEDIUM — official Cloudflare product documentation is current; the existing Vinext runtime is explicitly still under active development.

## Recommended Stack

Build the showcase as a Cloudflare-hosted, static-first editorial application. Keep the existing TypeScript/React visual prototype and Cloudflare Worker deployment for the launch; add D1 only for editorial metadata and R2 for every publishable image. The authoring interface is a small private `/studio` surface protected at the edge, not a general-purpose CMS.

### Core Framework

| Technology | Version / policy | Purpose | Why |
|---|---|---|---|
| TypeScript | strict; current Node 22 LTS-compatible toolchain | Site and studio code | Matches the prototype and produces shared, typed content contracts. |
| React 19 + existing Next-style App Router on Vinext | Keep current pinned compatibility set for launch; upgrade together only after `vinext check` and a deploy preview | Public routes, layouts, and the simple author studio | Avoids an expensive visual rewrite. The public site only needs the mature subset already exercised: routes, static rendering, Markdown display, and narrow route handlers. |
| Cloudflare Workers + Vite plugin | Current Wrangler/Vite versions locked in `package-lock.json` | Edge runtime, deployment, public static assets, and authenticated studio endpoints | The prototype is already built for it. Workers deploy code and static assets together and cache static assets globally. |
| Tailwind CSS 4 + existing CSS/GSAP | Preserve existing packages | Brand system and restrained motion | The visual language is already the asset to protect; do not replace it with a component library. |

### Content and Data

| Technology | Version / policy | Purpose | Why |
|---|---|---|---|
| Cloudflare D1 (SQLite) | One production database; versioned SQL migrations | Canonical editorial catalog: records, status, tags, people, teams, topics, asset references, publish dates, and ordering | A one-author catalog has low write volume and relational discovery needs. D1 is directly bound to the Worker; Drizzle already exists in the prototype. |
| Drizzle ORM + Drizzle Kit | Retain installed versions initially | Schema, migration generation, and typed database access | Makes the D1 schema reviewable and portable. Commit migrations, and apply them by immutable D1 database name—not merely the binding name. |
| Markdown (`.md`) + front matter | Existing archive imported first; editor source stored in D1 after studio ships | Article body format | Markdown is portable, diffable, and already used by the prototype. Preserve original source in Git during migration; store an immutable revision/snapshot with each published record. |
| Cloudflare R2, Standard storage | One `mallitalytics-media` bucket; immutable object keys | Original cards, figures, article heroes, social selects, and derivatives | This is the durable media boundary the project needs. R2 is S3-compatible, strongly consistent, and designed for public unstructured objects; its included monthly usage is appropriate for a low-traffic showcase. |

### Infrastructure and Publishing

| Technology | Purpose | Why |
|---|---|---|
| Cloudflare Access | Protect `/studio/*` and studio API routes with the author's existing identity provider / allowlist | Removes the need to build, operate, recover, or expose application login for one author. Keep public routes anonymous. |
| R2 custom media domain (for example `media.mallitalytics.com`) | Public, cacheable reads for published images | Use a custom domain rather than `r2.dev`, which Cloudflare documents as non-production. Do not expose bucket listing. |
| Worker R2 binding | Server-side media reads and privileged operations | No permanent R2 credentials in the browser. Uploads may use short-lived, scoped S3 presigned PUT URLs only after the Access-protected studio validates a requested key/type. |
| GitHub Actions or explicit local deploy | Build, test, deploy, and run D1 migrations | Keep production credentials in CI/Cloudflare secrets and make releases reproducible. Manual editorial publishing changes content data, not application code. |
| MLB Ops publish-candidate adapter (later) | A signed, narrow ingestion route or queue export, not direct database access | MLB Ops produces `candidate` records/assets. Only an author moves candidates to `published`; the public site never reads the private MLB warehouse or VPS filesystem. |

### Supporting Libraries

| Library | Purpose | When to use |
|---|---|---|
| `drizzle-orm`, `drizzle-kit` | D1 schema/access/migrations | Add real editorial tables; do not keep schema in ad-hoc JSON. |
| `marked` | Markdown rendering | Retain only with strict sanitization/allowlisted output before rendering author or future imported content. |
| `zod` | Request/front-matter/candidate validation | Add for every studio mutation and MLB Ops import payload. |
| `@aws-sdk/client-s3`, `@aws-sdk/s3-request-presigner` | Scoped direct R2 uploads | Add only when browser uploads are needed; omit if the first studio upload proxies small files through the Worker. |
| `sharp` | Build-time/local WebP thumbnails and metadata extraction | Use in a Node/CI/local preprocess step, not in a Worker request path and not as a runtime image CDN dependency. |

## Ownership Boundaries

```text
MLB Ops (private VPS / warehouse / renderers)
  -> approved asset + candidate metadata
  -> Showcase ingestion boundary (future, authenticated)

Mallitalytics Showcase Worker
  -> D1: editorial truth and publication state
  -> R2: immutable media objects
  -> public read routes / Access-protected studio routes

Browser
  -> public catalog and custom-domain media only
```

- MLB Ops owns analysis, data refresh, generated source artifacts, and draft candidates. It never receives the public site's deploy credentials or D1 token.
- The showcase owns editorial titles, slugs, categories, captions, attribution, asset selection, publishing state, and public URLs.
- R2 owns binary media; D1 stores object keys, dimensions, alt text, checksum, and usage references—never blobs or baseball warehouse data.
- `draft`, `review`, `published`, `archived`, and `rejected` must be explicit states. Public queries filter to `published` plus `published_at <= now` at the server boundary.

## Migration Plan: Existing Vinext/Sites Prototype

1. Keep the current public routes and CSS intact. Move only the existing `public/cards/` and `public/media/` files into R2 with stable immutable keys such as `cards/2026/05/<slug>-<sha>.webp`; retain a checked-in manifest during the transition.
2. Create D1 tables for `content_items`, `content_revisions`, `assets`, `tags`, and join tables for player/team/topic. Import the current Markdown front matter and `content/cards/manifest.json`; preserve the original Git files as migration source and backup rather than immediately deleting them.
3. Change public list/detail loaders from filesystem discovery to D1 queries plus R2 custom-domain URLs. Preserve old slugs and add permanent redirects for any changed route.
4. Add the narrow Access-protected studio: create/edit metadata, attach existing R2 assets, upload optimized images, preview, and explicitly publish/unpublish. This is enough for manual publishing.
5. Only after the manual workflow is reliable, let MLB Ops submit *candidates* through a separate authenticated endpoint. It cannot set `published` and cannot overwrite author-curated content.

## Alternatives Considered

| Category | Recommended | Alternative | Why not now |
|---|---|---|---|
| Public runtime | Existing Vinext + Cloudflare Workers | Rewrite now to a different framework | The visual prototype already runs here. A rewrite creates no editorial value before the archive and studio exist. |
| Long-term framework fallback | Standard Next.js deployment / OpenNext migration if Vinext compatibility becomes a blocker | Continue adding advanced App Router features to Vinext | Vinext's own documentation says it is under active development and flags gaps in cache components, build-time image/font optimization, native modules, and newer Next behavior. Keep features conservative and reassess before advanced dynamic work. |
| Editorial database | D1 + Drizzle | Postgres/Supabase/Neon | Premature operational complexity and cost for one author and modest catalog queries. Introduce Postgres only for demonstrable D1 limits, collaborative workflow, or a paid-product evolution. |
| Authentication | Cloudflare Access allowlist | Better Auth / custom password login | A single author needs an edge gate, not account management. Better Auth adds session and credential lifecycle work; primary documentation evidence for direct D1 support was not sufficient for this recommendation. |
| Media | R2 custom-domain objects | Git repository assets, Cloudflare Images, Google Drive hotlinks | Git history/builds should not grow with card archives; Cloudflare Images is unnecessary while assets are pre-rendered WebP; Drive is not a public CDN or media origin. |
| CMS | Purpose-built studio | Sanity/Contentful/WordPress | A hosted CMS duplicates the simple content model, splits ownership, and adds billing/vendor workflow before its collaboration features are needed. |

## Do Not Use in This Phase

- Do not make the public site call the MLB Ops FastAPI, warehouse, VPS, or Google Drive. They are private operational systems, not a public content origin.
- Do not put R2 write credentials, service tokens, or a broad presigning secret in client code. Treat presigned URLs as bearer tokens; constrain key, operation, MIME type, CORS origin, and short expiry.
- Do not use `r2.dev` as the production media URL, a mutable key such as `latest.webp` for archival work, or public listing conventions.
- Do not deploy a general reader-account, comment, payment, live-score, or dashboard stack. These violate the initial editorial-showcase scope and complicate moderation/security.
- Do not adopt advanced Vinext/Next cache or image-optimization features merely because they exist. Pre-render assets and use explicit D1/R2 reads until compatibility is proven by tests and preview deploys.

## Installation / Configuration Direction

```bash
# Existing app dependencies remain; add only when creating the studio boundary
npm install zod

# Direct browser-to-R2 upload, only if required after the studio MVP
npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner

# Generate typed Cloudflare bindings after configuring D1/R2 in Wrangler
npx wrangler types
```

Use a real `wrangler.jsonc`/`wrangler.toml` for production bindings, database identifiers, and migration configuration even though the Vite plugin can generate asset configuration. Keep non-secret IDs in configuration; store tokens and signing secrets in Cloudflare/CI secrets.

## Confidence and Sources

| Recommendation | Confidence | Basis |
|---|---|---|
| Workers + static assets, D1, and R2 are suitable for this launch | HIGH | Current official Cloudflare docs confirm Worker asset deployment/caching, D1 bindings/migrations, and R2's S3-compatible durable object storage. |
| R2 custom-domain media with immutable keys | HIGH | Official R2 docs specify public custom domains, `r2.dev`'s non-production role, and no egress charge; immutable keys are an archival design recommendation. |
| Edge-protected one-author studio rather than application auth | MEDIUM | Strong scope fit; the exact Access identity-provider configuration should be confirmed in implementation. |
| Retain Vinext only as a conservative bridge, with a migration escape hatch | HIGH | Vinext's own repository states it is under active development and lists relevant compatibility gaps. |
| D1 rather than Postgres for the initial catalog | MEDIUM | Matches workload and deployment topology; revisit after real catalog/query/automation scale is known. |

### Sources

- [Cloudflare Workers static assets](https://developers.cloudflare.com/workers/static-assets/) and [Vite plugin static assets](https://developers.cloudflare.com/workers/vite-plugin/reference/static-assets/)
- [Cloudflare D1 Worker API](https://developers.cloudflare.com/d1/worker-api/) and [D1 migrations](https://developers.cloudflare.com/d1/reference/migrations/)
- [Cloudflare R2: how it works](https://developers.cloudflare.com/r2/how-r2-works/), [public buckets](https://developers.cloudflare.com/r2/buckets/public-buckets/), [presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/), and [pricing](https://developers.cloudflare.com/r2/pricing/)
- [Vinext project status and known gaps](https://github.com/cloudflare/vinext)

*Source confidence in the research seam was classified LOW because its generic `websearch` provider classification does not distinguish official sources; the recommendations above elevate only claims directly verified against the linked official documentation.*
