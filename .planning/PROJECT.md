# Mallitalytics Showcase

## What This Is

Mallitalytics is a brand-led baseball publication and showcase of Gilberto Rojas's analytical craft. It turns the strongest articles, pitching cards, visual studies, trackers, dashboards, projects, and selected social work into a browsable body of work for baseball fans, while demonstrating clear analytical credibility to analysts, media, teams, and potential collaborators.

It is not a generic feed or a personal résumé. It is a living display window for the work: a visitor should be able to discover something useful about baseball and, at the same time, understand the perspective, quality, and craft behind Mallitalytics.

## Core Value

Every visitor can quickly see what Mallitalytics makes, why its baseball analysis is worth attention, and how to explore its best and most recent work.

## Requirements

### Validated

- ✓ Mallitalytics has a public English-language website with a distinctive editorial visual system — existing site
- ✓ Mallitalytics produces baseball articles, pitching cards, visual research, trackers, and dashboard-oriented analysis — existing MLB Ops workflow
- ✓ Mallitalytics has original brand assets and a documented visual identity — existing brand system

### Active

- [ ] Make the public site a coherent editorial showcase rather than a static concept page
- [ ] Curate and present a launch collection of the strongest existing Mallitalytics projects and content
- [ ] Create durable homes for articles, visual cards, projects, selected tweets, and recurring trackers
- [ ] Let the author publish and organize new content without editing website code
- [ ] Give visitors clear paths from featured work to browsable archives by content type, date, player, team, and topic where relevant
- [ ] Make Mallitalytics authorship, methodology, and point of view visible without making the site a personal résumé
- [ ] Establish a safe path from manual editorial publishing to future MLB Ops-assisted publishing

### Out of Scope

- Subscriptions, paywalls, and payments — the immediate purpose is visibility, craft, and audience-building, not monetization
- Reader accounts, comments, and community features — unnecessary for a one-author showcase and would dilute the editorial focus
- Betting advice or paid picks — the site should demonstrate analytical work without making gambling conversion its purpose
- Mobile-native application — the public web experience is the priority
- Automated publishing as the first release — editorial control and a usable manual workflow come first

## Context

- A public Mallitalytics prototype already exists in `mallitalytics-public/`; it establishes the visual direction but currently behaves as a static page.
- MLB Ops already generates original baseball outputs, including pitching cards, league studies, trackers, daily boards, dashboards, and editorial material. These are the source collection for the showcase.
- Reference sites include TJ Stats and Poised Bet: their useful qualities are clear content structure and frequent data-led publishing, not their full business models.
- The initial audience is both baseball fans and baseball-industry readers. Content must stay readable without sacrificing methodological credibility.
- The initial launch should curate the strongest existing work rather than blindly import every historical artifact. The archive can deepen over time.
- The site will initially be a non-commercial project. It may later evolve into a subscription or premium product, but that possibility must not distort the first release.

## Constraints

- **Language**: English-only — this is the current public-facing requirement.
- **Brand**: Mallitalytics leads; Gilberto's authorship is clear but secondary to the brand.
- **Editorial control**: New content needs a private, low-friction publishing path — manual publishing must work before automation is introduced.
- **Storage**: Visual assets must live in durable storage instead of accumulating indefinitely inside the website repository.
- **Source integration**: MLB Ops can become a publishing source later, but external automation must not publish unreviewed work.
- **Scope**: Preserve the existing visual quality while adding real content behavior incrementally.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Position Mallitalytics as an editorial showcase and data publication | This combines the user's craft portfolio goal with the value of useful baseball content | — Pending |
| Serve baseball fans and industry readers | The work should be discoverable and readable while functioning as credible proof of analytical capability | — Pending |
| Launch with a curated existing collection | Selection communicates taste and avoids an unstructured historical dump | — Pending |
| Start manual, then add MLB Ops automation | Editorial judgment must remain in control while the publishing model proves itself | — Pending |
| Defer monetization | The near-term goal is a useful public showcase and archive | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-08-10 after project-purpose definition*
