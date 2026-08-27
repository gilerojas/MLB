"""Phase 3 (L2) -- represent each pitcher-season as a distribution over shape space.

L1 established that pitch shape is a continuum with no natural family boundaries
(RESEARCH_LOG L1-F5), so hard family labels are off the table (L1-D6). What
survives is *position*: a pitcher can still be described by where on the surface
his pitches live, the way a zip code describes where somebody lives without
claiming a wall runs down the street.

The soft basis below is exactly that -- a coordinate system, not a taxonomy. A
16-component Gaussian mixture is fit once over all pooled seasons, and each pitch
gets a vector of membership weights rather than a label. A pitcher-season is then
the average of those vectors: a distribution over the surface. Nothing here
asserts the 16 components are real pitch types, and no downstream phase may treat
them as such.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/build_arsenal_profiles.py

Writes outputs/l2_arsenal_profiles.parquet (one row per pitcher-season),
outputs/l2_basis_centroids.csv (the coordinate system, in readable units), and
outputs/l2_fingerprint_check.csv (the L2-F1 validation).
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.features import SEED  # noqa: E402

CACHE = ROOT / "research/study/.cache"
OUT = Path(__file__).resolve().parent / "outputs"

BLOCK = ["release_speed", "pfx_x_mir_in", "pfx_z_in", "release_spin_rate"]
JUNK_TYPES = {"EP", "PO", "SC", "UN", "FA", "CS", "KN", "FO"}

# Basis resolution. Not a claim about how many pitch types exist -- L1 says none
# do. Chosen as the point where the L1 sweep's stability had clearly broken down
# (ARI < 0.9), i.e. fine enough to resolve arsenal shape, coarse enough that each
# cell still holds thousands of pitches.
K_BASIS = 16
N_BASIS_FIT = 150_000

# A pitcher-season below this has too few pitches for a stable distribution.
MIN_PITCHES = 400


def load_pitches(tag: str) -> pd.DataFrame:
    path = CACHE / f"archetype_pitches_{tag}.parquet"
    if not path.exists():
        raise SystemExit(
            f"missing {path}\nrun: build_pitch_dataset.py --years {tag.replace('-', ' ')}")
    df = pd.read_parquet(path)
    df = df.dropna(subset=BLOCK + ["pitch_type", "pitcher"])
    df = df[~df["pitch_type"].isin(JUNK_TYPES)]
    # The cache names the season `game_year`; the study calls it `season`.
    df["season"] = df["game_year"]
    return df.reset_index(drop=True)


def fit_basis(df: pd.DataFrame, rng: np.random.Generator):
    """Fit the soft coordinate system once, over all seasons pooled.

    Pooling matters: a basis refit per season would make coordinates
    incomparable across years and would silently destroy the year-over-year
    persistence test that L2-F1 and L3 both depend on.
    """
    idx = rng.choice(len(df), size=min(N_BASIS_FIT, len(df)), replace=False)
    scaler = StandardScaler().fit(df.iloc[idx][BLOCK].to_numpy(dtype="float64"))
    X = scaler.transform(df.iloc[idx][BLOCK].to_numpy(dtype="float64"))
    gmm = GaussianMixture(n_components=K_BASIS, covariance_type="full",
                          n_init=3, random_state=SEED).fit(X)
    return scaler, gmm


def basis_centroids(scaler, gmm) -> pd.DataFrame:
    """The coordinate system in readable units, so profiles are interpretable."""
    raw = scaler.inverse_transform(gmm.means_)
    out = pd.DataFrame(raw, columns=BLOCK).round(2)
    out.insert(0, "cell", [f"c{i:02d}" for i in range(len(out))])
    out["weight"] = gmm.weights_.round(4)
    return out


def responsibilities(df: pd.DataFrame, scaler, gmm) -> np.ndarray:
    """Soft membership of every pitch, in chunks to keep memory flat."""
    n = len(df)
    R = np.empty((n, K_BASIS), dtype="float32")
    step = 250_000
    for s in range(0, n, step):
        e = min(s + step, n)
        X = scaler.transform(df.iloc[s:e][BLOCK].to_numpy(dtype="float64"))
        R[s:e] = gmm.predict_proba(X).astype("float32")
    return R


def build_profiles(df: pd.DataFrame, R: np.ndarray) -> pd.DataFrame:
    cells = [f"c{i:02d}" for i in range(K_BASIS)]
    work = df[["pitcher", "pitcher_name", "season", "p_throws", "stand",
               "is_starter", "game_pk", "release_speed", "pfx_x_mir_in",
               "pfx_z_in", "arm_angle", "release_extension",
               "release_pos_x_mir", "release_pos_z"]].copy()
    work[cells] = R
    key = ["pitcher", "season"]

    # Overall distribution over the surface.
    prof = work.groupby(key, observed=True)[cells].mean()

    # Split by batter handedness. Half the original intuition -- the lefty who
    # lives off a changeup only does so against right-handed hitters -- lives
    # entirely in this split, and it is invisible in the pooled vector.
    for hand in ("L", "R"):
        sub = work[work["stand"] == hand].groupby(key, observed=True)[cells].mean()
        prof = prof.join(sub.add_suffix(f"_vs{hand}"), how="left")

    # Pitcher-identity traits, held out of L1 by design (L0-D3) because they
    # describe the pitcher rather than the pitch. This is where they belong.
    ident = work.groupby(key, observed=True).agg(
        arm_angle=("arm_angle", "median"),
        extension=("release_extension", "median"),
        release_x=("release_pos_x_mir", "median"),
        release_z=("release_pos_z", "median"),
        n_pitches=("release_speed", "size"),
        n_games=("game_pk", "nunique"),
        starter_share=("is_starter", "mean"),
        # 2026 ships almost no arm_angle (4% vs 99% in 2024/25), so the median
        # above is drawn from a handful of pitches for those rows. The coverage
        # is carried per row rather than imputed, and L3 must gate on it.
        arm_angle_cov=("arm_angle", lambda s: float(s.notna().mean())),
        p_throws=("p_throws", "first"),
        pitcher_name=("pitcher_name", "first"),
    )

    # Velocity envelope. "Power vs crafty" is largely a statement about how hard
    # the top of the arsenal is and how far the bottom sits from it.
    velo = work.groupby(key, observed=True)["release_speed"].agg(
        fb_velo=lambda s: s.quantile(0.95),
        slow_velo=lambda s: s.quantile(0.10),
    )
    velo["velo_spread"] = (velo["fb_velo"] - velo["slow_velo"]).round(2)

    prof = prof.join(ident).join(velo.round(2))

    # Arsenal breadth as effective number of occupied cells. exp(entropy) is
    # readable as "how many pitches does he really have", and unlike a raw count
    # of pitch_types it does not depend on the label conventions L1 discredited.
    P = prof[cells].to_numpy(dtype="float64")
    P = np.clip(P, 1e-12, None)
    prof["arsenal_breadth"] = np.exp(-(P * np.log(P)).sum(axis=1)).round(3)

    # How far apart the arsenal is spread on the movement plane.
    disp = work.groupby(key, observed=True).agg(
        mov_x_sd=("pfx_x_mir_in", "std"), mov_z_sd=("pfx_z_in", "std"))
    prof = prof.join(disp.round(2))

    prof = prof[prof["n_pitches"] >= MIN_PITCHES].reset_index()
    return prof


def fingerprint_check(prof: pd.DataFrame) -> pd.DataFrame:
    """L2-F1 -- is this representation a fingerprint or noise?

    A pitcher's profile in one season must resemble *his own* profile in the next
    season more than it resembles a random other pitcher's. If it does not, the
    vector is measuring noise and nothing downstream can be trusted. Distance is
    cosine on the pooled cell distribution.
    """
    cells = [f"c{i:02d}" for i in range(K_BASIS)]
    V = prof[cells].to_numpy(dtype="float64")
    V = V / np.linalg.norm(V, axis=1, keepdims=True)
    prof = prof.reset_index(drop=True)

    pairs = []
    by_pitcher = prof.groupby("pitcher", observed=True).groups
    for pid, rows in by_pitcher.items():
        rows = sorted(rows, key=lambda r: prof.at[r, "season"])
        for a, b in zip(rows, rows[1:]):
            pairs.append((a, b))
    if not pairs:
        raise SystemExit("no pitcher appears in two consecutive seasons")

    same = np.array([float(V[a] @ V[b]) for a, b in pairs])

    rng = np.random.default_rng(SEED)
    diff = []
    for a, _ in pairs:
        for _ in range(5):
            b = rng.integers(len(prof))
            if prof.at[b, "pitcher"] != prof.at[a, "pitcher"]:
                diff.append(float(V[a] @ V[b]))
    diff = np.array(diff)

    rows = [{
        "comparison": "same pitcher, consecutive seasons",
        "n": len(same), "mean_cosine": round(float(same.mean()), 4),
        "sd": round(float(same.std()), 4),
    }, {
        "comparison": "different pitchers",
        "n": len(diff), "mean_cosine": round(float(diff.mean()), 4),
        "sd": round(float(diff.std()), 4),
    }]
    pooled = np.sqrt((same.var() + diff.var()) / 2)
    rows.append({
        "comparison": "separation (Cohen's d)",
        "n": len(same) + len(diff),
        "mean_cosine": round(float((same.mean() - diff.mean()) / pooled), 4),
        "sd": None,
    })
    return pd.DataFrame(rows)


def main() -> None:
    OUT.mkdir(exist_ok=True)
    rng = np.random.default_rng(SEED)

    df = load_pitches("2024-2025-2026")
    print(f"loaded {len(df):,} pitches, seasons {sorted(df['season'].unique())}")

    scaler, gmm = fit_basis(df, rng)
    cent = basis_centroids(scaler, gmm)
    cent.to_csv(OUT / "l2_basis_centroids.csv", index=False)
    print(f"\nsoft basis ({K_BASIS} cells, a coordinate system -- not pitch types):")
    print(cent.to_string(index=False))

    R = responsibilities(df, scaler, gmm)
    prof = build_profiles(df, R)
    print(f"\n{len(prof)} pitcher-seasons with >= {MIN_PITCHES} pitches "
          f"({prof['pitcher'].nunique()} pitchers)")
    print(f"  starters (>=80% of pitches as SP): {(prof['starter_share'] >= 0.8).sum()}")
    print("\nper season (arm_angle_cov exposes the 2026 ingest gap):")
    print(prof.groupby("season").agg(
        rows=("pitcher", "size"),
        med_pitches=("n_pitches", "median"),
        starters=("starter_share", lambda s: int((s >= 0.8).sum())),
        arm_angle_cov=("arm_angle_cov", "median"),
    ).round(3).to_string())

    prof.to_parquet(OUT / "l2_arsenal_profiles.parquet", index=False)
    prof.to_csv(OUT / "l2_arsenal_profiles.csv", index=False)

    fp = fingerprint_check(prof)
    print("\nL2-F1 -- fingerprint check (cosine similarity of cell distributions):")
    print(fp.to_string(index=False))
    fp.to_csv(OUT / "l2_fingerprint_check.csv", index=False)

    print(f"\noutputs -> {OUT}")


if __name__ == "__main__":
    main()
