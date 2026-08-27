"""Phase 4 (L3) -- do *pitchers* cluster?

The question the study exists to answer. L1 showed individual pitches lie on a
continuum with no natural families. That does not settle this: arsenal
*combinations* are constrained by what a human body can actually do -- you cannot
pair an elite sweeper with an elite splitter from the same slot at the same
effort -- and those constraints could produce real pitcher groups on a perfectly
smooth pitch surface. L2 then showed arsenal profiles are stable, distinctive
fingerprints (cosine 0.936 within a pitcher vs 0.475 between), which is the
precondition this level needs.

Fingerprints being distinctive is not the same as their falling into groups. No
two fingerprints are alike either. This phase runs the same null discipline as
L1-F5 and is allowed to return the same negative.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/cluster_pitchers.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score, silhouette_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.features import SEED  # noqa: E402

OUT = Path(__file__).resolve().parent / "outputs"

CELLS = [f"c{i:02d}" for i in range(16)]
COMPOSITIONAL = [CELLS, [f"{c}_vsL" for c in CELLS], [f"{c}_vsR" for c in CELLS]]
SCALAR = ["arm_angle", "extension", "release_x", "release_z",
          "fb_velo", "velo_spread", "arsenal_breadth", "mov_x_sd", "mov_z_sd"]

SEASONS = (2024, 2025)          # 2026 excluded: arm_angle 3.6% covered, season partial
MIN_ARM_ANGLE_COV = 0.5
STARTER_SHARE = 0.8
K_GRID = list(range(2, 13))
N_BOOT = 25
N_PCA = 10


def clr(block: np.ndarray) -> np.ndarray:
    """Centered log-ratio.

    The cell vectors are compositions -- they sum to one, so their components are
    not free to vary independently and plain Euclidean distance on them is not
    meaningful. CLR maps a composition to real space where it is. GMM
    responsibilities are strictly positive, so no zero-handling is needed beyond
    a floor against underflow.
    """
    x = np.clip(block, 1e-9, None)
    lg = np.log(x)
    return lg - lg.mean(axis=1, keepdims=True)


def load_matrix() -> tuple[pd.DataFrame, np.ndarray]:
    prof = pd.read_parquet(OUT / "l2_arsenal_profiles.parquet")
    prof = prof[prof["season"].isin(SEASONS)]
    prof = prof[prof["starter_share"] >= STARTER_SHARE]
    prof = prof[prof["arm_angle_cov"] >= MIN_ARM_ANGLE_COV]
    prof = prof.dropna(subset=SCALAR + [c for b in COMPOSITIONAL for c in b])

    # One row per *pitcher*, not per pitcher-season. L2-F1 measured within-pitcher
    # similarity at cosine 0.936, so a pitcher's two seasons are near-duplicates.
    # Left as separate rows they would leak across every bootstrap split and
    # inflate stability for reasons having nothing to do with clustering.
    num = [c for b in COMPOSITIONAL for c in b] + SCALAR
    agg = prof.groupby("pitcher", observed=True).agg(
        {**{c: "mean" for c in num},
         "pitcher_name": "first", "p_throws": "first",
         "n_pitches": "sum", "season": "nunique"})
    agg = agg.rename(columns={"season": "n_seasons"}).reset_index()

    blocks = [clr(agg[b].to_numpy(dtype="float64")) for b in COMPOSITIONAL]
    blocks.append(agg[SCALAR].to_numpy(dtype="float64"))
    X = StandardScaler().fit_transform(np.hstack(blocks))
    return agg, X


def _labels(X: np.ndarray, k: int, seed: int) -> np.ndarray:
    """Reduce then cluster. PCA is refit on whatever rows it is given, so when
    this is called inside a resampling loop the reduction is refit too -- fitting
    it once on all rows would let the held-out half inform its own projection."""
    Z = PCA(n_components=min(N_PCA, X.shape[0] - 1, X.shape[1])).fit_transform(X)
    return KMeans(k, n_init=10, random_state=seed).fit_predict(Z)


def bootstrap_ari(X: np.ndarray, k: int) -> float:
    aris = []
    for b in range(N_BOOT):
        r = np.random.default_rng(SEED + b)
        a_idx = r.choice(len(X), size=int(len(X) * 0.7), replace=False)
        b_idx = r.choice(len(X), size=int(len(X) * 0.7), replace=False)
        overlap = np.intersect1d(a_idx, b_idx)
        if len(overlap) < 30:
            continue
        la = _labels(X[a_idx], k, SEED + b)
        lb = _labels(X[b_idx], k, SEED + b)
        pa = {v: i for i, v in enumerate(a_idx)}
        pb = {v: i for i, v in enumerate(b_idx)}
        aris.append(adjusted_rand_score(la[[pa[o] for o in overlap]],
                                        lb[[pb[o] for o in overlap]]))
    return float(np.mean(aris))


def null_columns(X: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Null A -- shuffle every column independently.

    Preserves each feature's marginal distribution, destroys all joint structure.
    Caveat, and it matters: this also destroys the compositional geometry the CLR
    blocks carry, which is itself real structure. So Null A is *generous* to the
    real data -- it can manufacture an excess where none exists. A negative
    result against it is therefore strong; a positive one is not sufficient, and
    is why Null B exists.
    """
    Xn = X.copy()
    for j in range(Xn.shape[1]):
        Xn[:, j] = rng.permutation(Xn[:, j])
    return Xn


def null_blocks(X: np.ndarray, widths: list[int], rng: np.random.Generator) -> np.ndarray:
    """Null B -- shuffle whole blocks across pitchers, keeping each block intact.

    Every pitcher gets some real pitcher's pooled arsenal, some *other* real
    pitcher's vsL arsenal, and so on. Each block keeps its internal geometry
    exactly; only the association *between* blocks is destroyed. This is the
    strict test: is there structure in how the pieces of a pitcher go together,
    beyond what each piece looks like on its own?
    """
    Xn = X.copy()
    start = 0
    for w in widths:
        Xn[:, start:start + w] = Xn[rng.permutation(len(Xn)), start:start + w]
        start += w
    return Xn


def null_gaussian(X: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """Null C -- one multivariate Gaussian with the real data's mean and covariance.

    The decisive test, and the only one that isolates the actual question.
    Nulls A and B destroy correlation, so beating them only proves the features
    co-vary -- which is certain and uninteresting. Null C keeps every correlation
    exactly and is, by construction, a single unimodal blob with no clusters
    anywhere in it. If real pitchers partition no more stably than one Gaussian
    cloud, then whatever structure the features carry is *continuous*, and the
    groups k-means returns are slices of a cloud rather than discovered types.
    """
    mu = X.mean(axis=0)
    cov = np.cov(X, rowvar=False)
    return rng.multivariate_normal(mu, cov, size=len(X))


def main() -> None:
    OUT.mkdir(exist_ok=True)
    rng = np.random.default_rng(SEED)

    agg, X = load_matrix()
    print(f"{len(agg)} starters ({SEASONS[0]}-{SEASONS[-1]}), {X.shape[1]} features")
    print(f"  seen in both seasons: {(agg['n_seasons'] == 2).sum()}")

    widths = [len(b) for b in COMPOSITIONAL] + [len(SCALAR)]
    XnA = null_columns(X, rng)
    XnB = null_blocks(X, widths, rng)
    XnC = null_gaussian(X, rng)

    Z = PCA(n_components=N_PCA).fit(X)
    print(f"  PCA {N_PCA} comps retain {Z.explained_variance_ratio_.sum():.1%} of variance")

    rows = []
    for k in K_GRID:
        real = bootstrap_ari(X, k)
        na = bootstrap_ari(XnA, k)
        nb = bootstrap_ari(XnB, k)
        nc = bootstrap_ari(XnC, k)
        sil = silhouette_score(Z.transform(X), _labels(X, k, SEED))
        sil_c = silhouette_score(PCA(N_PCA).fit_transform(XnC), _labels(XnC, k, SEED))
        rows.append({"k": k, "silhouette": round(sil, 4),
                     "silhouette_null_gauss": round(sil_c, 4),
                     "ari_real": round(real, 4),
                     "ari_null_columns": round(na, 4),
                     "ari_null_blocks": round(nb, 4),
                     "ari_null_gaussian": round(nc, 4),
                     "excess_vs_columns": round(real - na, 4),
                     "excess_vs_blocks": round(real - nb, 4),
                     "excess_vs_gaussian": round(real - nc, 4)})
        print(f"  k={k:2d}  sil={sil:.3f} (gauss {sil_c:.3f})  real={real:.3f}  "
              f"A={na:.3f} ({real-na:+.3f})  B={nb:.3f} ({real-nb:+.3f})  "
              f"C={nc:.3f} ({real-nc:+.3f})", flush=True)

    res = pd.DataFrame(rows)
    res.to_csv(OUT / "l3_cluster_stability.csv", index=False)

    best = res.sort_values("excess_vs_gaussian", ascending=False).iloc[0]
    print(f"\nbest excess vs the decisive null (C, one Gaussian): "
          f"k={int(best['k'])} ({best['excess_vs_gaussian']:+.3f})")
    print(f"mean excess vs C across k: {res['excess_vs_gaussian'].mean():+.3f}; "
          f"negative at k = "
          f"{sorted(res.loc[res['excess_vs_gaussian'] < 0, 'k'].tolist())}")

    # Characterise whatever the best k gives, so the result is inspectable
    # regardless of whether it survived the nulls.
    k = int(best["k"])
    agg = agg.copy()
    agg["cluster"] = _labels(X, k, SEED)
    summary = agg.groupby("cluster").agg(
        n=("pitcher", "size"),
        lhp=("p_throws", lambda s: round((s == "L").mean(), 2)),
        arm_angle=("arm_angle", "mean"), fb_velo=("fb_velo", "mean"),
        velo_spread=("velo_spread", "mean"), breadth=("arsenal_breadth", "mean"),
        extension=("extension", "mean"),
    ).round(2)
    summary["examples"] = agg.groupby("cluster")["pitcher_name"].apply(
        lambda s: ", ".join(s.head(4)))
    print(f"\nk={k} groups (descriptive only -- see the nulls above):")
    print(summary.to_string())
    summary.to_csv(OUT / "l3_cluster_summary.csv")
    agg[["pitcher", "pitcher_name", "p_throws", "cluster", "arm_angle", "fb_velo",
         "velo_spread", "arsenal_breadth"]].to_csv(
        OUT / "l3_pitcher_assignments.csv", index=False)
    print(f"\noutputs -> {OUT}")


if __name__ == "__main__":
    main()


def describe_axes(agg: pd.DataFrame, X: np.ndarray, n: int = 4) -> pd.DataFrame:
    """L3-F2 -- if the cloud has no clusters, what are its axes?

    A continuum still has shape. Each principal component is correlated against
    the interpretable pitcher-level features so the axes can be named and used as
    coordinates, which is what replaces the archetype label that L3-F1 refused.
    """
    p = PCA(n_components=n).fit(X)
    Z = p.transform(X)
    rows = []
    for i in range(n):
        row = {"axis": f"PC{i+1}",
               "explained_var": round(float(p.explained_variance_ratio_[i]), 4)}
        for f in SCALAR:
            row[f] = round(float(np.corrcoef(Z[:, i], agg[f])[0, 1]), 3)
        row["is_lhp"] = round(float(np.corrcoef(
            Z[:, i], (agg["p_throws"] == "L").astype(float))[0, 1]), 3)
        rows.append(row)
    return pd.DataFrame(rows)
