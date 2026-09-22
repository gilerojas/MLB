"""Shared writing contract for Mallitalytics AI-generated editorial copy."""

from __future__ import annotations


_CORE_PRINCIPLES = """Mallitalytics editorial contract:
- Lead with the strongest verified baseball insight, not praise, throat-clearing, or a template statistic.
- Make one clear claim at a time. Support it with the fewest concrete facts needed.
- Prefer short, familiar words and active voice. Cut any word that does not add meaning.
- Use baseball terms when they are the most exact words; do not use jargon to sound analytical.
- Avoid stale metaphors, generic hype, canned transitions, and conclusions that could fit any player or game.
- Treat observed changes as evidence, not causes. Never invent intent, causality, context, or missing data.
- Vary the doorway into the story: game shape, command, arsenal, velocity, contact, sequencing, or season context.
- Before returning copy, verify every factual claim, remove repetition, and tighten the opening and closing."""


_TWEET_RULES = """Tweet-specific edit:
- Give the post one primary angle. Other numbers may support it, but must not compete with it.
- The opening should contain a player, matchup, event, or meaningful fact within its first eight words.
- End on a concrete baseball point or a clean declarative sentence, not engagement bait or a generic question.
- Preserve natural rhythm. Sentence lengths may vary, but every sentence must earn its space."""


_NEWSLETTER_RULES = """Newsletter-specific edit:
- Prioritize what changes the reader's understanding of today's MLB slate or yesterday's results.
- Synthesize related facts instead of reciting every available section or JSON field.
- Use short paragraphs with a clear topic sentence and a useful final detail.
- Sound like an informed editor briefing a regular reader, not an assistant describing its research process."""


def mallitalytics_editorial_contract(output_kind: str = "general") -> str:
    """Return the stable editorial rules for the requested output surface."""
    kind = output_kind.strip().lower()
    if kind == "tweet":
        return f"{_CORE_PRINCIPLES}\n{_TWEET_RULES}"
    if kind == "newsletter":
        return f"{_CORE_PRINCIPLES}\n{_NEWSLETTER_RULES}"
    return _CORE_PRINCIPLES
