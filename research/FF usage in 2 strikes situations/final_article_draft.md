# The Trust Pitch: What Two-Strike Four-Seam Usage Tells Us About Fastball Quality

When building a pitching card, pitch usage by count can sometimes say more than season-level pitch mix. A pitcher may throw a four-seamer often overall, but the more interesting question is when he still trusts it.

That is what led to this case study:

Do pitchers who use more four-seam fastballs in two-strike counts tend to be more successful?

The intuition is simple. With two strikes, hitters are protecting. They are more willing to fight off velocity, shorten up, and cover more of the zone. If a pitcher keeps throwing the four-seamer in that situation, it probably means the pitch has traits that allow it to survive at the highest-leverage point of the plate appearance.

But the key word is probably. Usage alone is never enough.

## The Test

For each pitcher, I measured four-seam usage in two-strike counts:

`2-strike FF% = four-seamers thrown with two strikes / all two-strike pitches`

Then I compared that number against pitcher success and four-seam quality:

- K%
- BB%
- K-BB%
- run value per 100 pitches
- xwOBA on batted balls
- overall whiff rate
- CSW%
- four-seam velocity
- four-seam induced vertical break
- four-seam horizontal break
- four-seam whiff rate
- four-seam CSW%

There is one important methodological issue: this is a percentage stat, so denominator matters.

Thirty percent four-seam usage on 20 two-strike pitches is not the same level of evidence as 30% on 300 two-strike pitches. The rate may be identical, but the second pitcher has given us a much more stable signal.

To handle that, I looked at both unweighted correlations and correlations weighted by each pitcher's number of two-strike pitches.

## The Main Result

The relationship between two-strike four-seam usage and pitcher success is positive, but not massive.

In the 2025 local sample, `2-strike FF%` had:

- `+0.192` unweighted correlation with K-BB%
- `+0.199` weighted correlation with K-BB%
- `+0.191` unweighted correlation with K%
- `+0.201` weighted correlation with K%

That tells us two-strike four-seam usage is not a magic metric. It does not explain pitcher quality by itself.

But the more interesting part came when looking at four-seam traits.

In the same 2025 sample, `2-strike FF%` had:

- `+0.294` weighted correlation with four-seam velocity
- `+0.335` weighted correlation with four-seam IVB
- `+0.316` weighted correlation with four-seam CSW%
- almost no relationship with four-seam HB

That is the real finding.

Pitchers who keep using the four-seamer with two strikes tend to have four-seamers with better velocity, better vertical life, and better called-strike-plus-whiff performance.

## The Conclusion

Two-strike four-seam usage is not necessarily the cause of success.

It is more likely evidence of trust.

Pitchers do not become good simply because they throw more four-seamers with two strikes. They throw more four-seamers with two strikes because their four-seamer is good enough to survive in that count.

That distinction matters.

The wrong conclusion would be:

"Throw more fastballs with two strikes."

The better conclusion is:

"If a pitcher keeps throwing four-seamers with two strikes, check whether the pitch has the traits to justify that trust."

## Why IVB Matters Here

Adding IVB and HB helps explain the physics behind the trust signal.

In this sample, IVB carried a clearer relationship than HB. That makes sense for the classic two-strike four-seamer. The pitch often wins by staying above barrels, creating vertical separation, and giving hitters less time and less room to adjust.

Horizontal break still matters, especially for approach angle, handedness matchups, and location strategy. But in this specific test, the trust signal was more tied to ride than run.

That gives the pitching card a useful layer:

High two-strike FF usage is more interesting when it comes with velocity, IVB, and CSW. Without those supporting traits, high usage could be a warning sign instead of a strength.

## Four-Seam vs Fastball Family

Another useful finding: grouping four-seamers, sinkers, and cutters together weakened the signal.

That matters because "fastball" is too broad for this question.

A four-seamer, sinker, and cutter can all be hard pitches, but they often have different jobs:

- Four-seamers can win with ride, velocity, carry, and vertical approach.
- Sinkers often target weak contact, arm-side run, and ground-ball shapes.
- Cutters can manage barrels, steal zones, or bridge the gap between fastball and slider.

So for this case study, the cleanest lens is specifically the four-seamer.

## How I Would Use This on a Pitching Card

Two-strike FF% should not be treated as a grade by itself. It should be treated as a prompt for a better question.

If a pitcher has high two-strike FF usage, ask:

- How hard is the four-seamer?
- How much IVB does it have?
- Does it get whiffs?
- Does it get called strikes?
- Does it avoid damage?
- Is the pitcher using it in locations where those traits play?

The best version of the metric is not "high equals good."

The best version is:

High two-strike FF usage plus strong four-seam traits equals trust backed by evidence.

## Final Takeaway

Two-strike four-seam usage is a clue, not an answer.

It tells us something about what the pitcher believes he can still throw when the hitter is most defensive and the at-bat is closest to resolution.

When that trust is backed by velocity, IVB, and CSW, the metric becomes much more meaningful.

That is why it belongs on the pitching card.

Not because it tells the whole story, but because it points us toward the right story.

