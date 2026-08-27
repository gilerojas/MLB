"""Phase 2 -- Level 1 exploration: is there structure in pitch shape space?

This phase commits to nothing. It answers four questions that decide whether a
pitch-family taxonomy is real before any archetype is named:

  Q1  Does handedness mirroring work? In the raw catcher's frame, handedness is
      trivially recoverable from movement and would dominate any clustering.
      After mirroring it should be close to unrecoverable.
  Q2  Is the shape space clustered, or is it one continuum with velocity on the
      long axis? A continuum is a legitimate finding, not a failure.
  Q3  Which feature block? Movement-only, or movement plus spin axis -- which is
      the polar form of the same vector (r = 0.88 with pfx_x). Decided by
      stability, not by argument.
  Q4  How many families, and do they cut across the crowdsourced `pitch_type`
      labels? If empirical families reproduce the labels exactly, the whole
      exercise is redundant with a column we already have.

Stability is the arbiter throughout: a k whose labels do not survive resampling
is an artifact of k, not a property of baseball.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/explore_shape_space.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import adjusted_rand_score, roc_auc_score, silhouette_score
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.features import SEED  # noqa: E402

CACHE = ROOT / "research/study/.cache/archetype_pitches_2025.parquet"
OUT = Path(__file__).resolve().parent / "outputs"

# Fitting subsample. 712k pitches is far more than any of these estimators need,
# and silhouette is O(n^2). Seeded, so every rerun reproduces exactly.
N_FIT = 60_000
N_SIL = 15_000
N_BOOT = 5

BLOCK_MOVEMENT = ["release_speed", "pfx_x_mir_in", "pfx_z_in", "release_spin_rate"]
BLOCK_WITH_AXIS = BLOCK_MOVEMENT + ["spin_axis_sin", "spin_axis_cos"]
K_GRID = list(range(4, 19))

# Pitch types too rare to support a family of their own, or not really pitches.
JUNK_TYPES = {"EP", "PO", "SC", "UN", "FA", "CS", "KN", "FO"}


def load() -> pd.DataFrame:
    df = pd.read_parquet(CACHE)
    keep = BLOCK_WITH_AXIS + ["pfx_x", "pfx_z", "p_throws", "pitch_type",
                              "pitcher", "pitcher_name", "is_starter"]
    df = df[keep].dropna(subset=BLOCK_WITH_AXIS + ["pitch_type"])
    df = df[~df["pitch_type"].isin(JUNK_TYPES)]
    return df.reset_index(drop=True)


def q1_mirroring(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Can handedness be predicted from the features, before and after mirroring?

    AUC ~1.0 means the frame encodes handedness and clustering would recover it.
    AUC ~0.5 means the mirror worked and handedness is free to be a descriptor.
    """
    idx = rng.choice(len(df), size=min(60_000, len(df)), replace=False)
    d = df.iloc[idx]
    y = (d["p_throws"] == "L").to_numpy().astype(int)

    rows = []
    frames = {
        "raw_catcher_frame": ["release_speed", "pfx_x", "pfx_z", "release_spin_rate"],
        "mirrored_rhp_frame": BLOCK_MOVEMENT,
    }
    for name, cols in frames.items():
        X = d[cols].to_numpy(dtype="float64")
        pipe = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
        auc = cross_val_score(pipe, X, y, cv=4, scoring="roc_auc").mean()

        pcs = PCA(n_components=1).fit_transform(StandardScaler().fit_transform(X))
        rows.append({
            "frame": name,
            "handedness_auc": round(float(auc), 4),
            "abs_corr_pc1_lhp": round(abs(float(np.corrcoef(pcs[:, 0], y)[0, 1])), 4),
        })
    return pd.DataFrame(rows)


def q2_pca(df: pd.DataFrame, rng: np.random.Generator) -> tuple[pd.DataFrame, pd.DataFrame]:
    idx = rng.choice(len(df), size=N_FIT, replace=False)
    X = StandardScaler().fit_transform(df.iloc[idx][BLOCK_MOVEMENT].to_numpy(dtype="float64"))
    p = PCA().fit(X)
    var = pd.DataFrame({
        "component": [f"PC{i+1}" for i in range(len(p.explained_variance_ratio_))],
        "explained_var": p.explained_variance_ratio_.round(4),
        "cumulative": p.explained_variance_ratio_.cumsum().round(4),
    })
    load = pd.DataFrame(p.components_.T, index=BLOCK_MOVEMENT,
                        columns=[f"PC{i+1}" for i in range(p.n_components_)]).round(3)
    return var, load.reset_index(names="feature")


def _fit_labels(X: np.ndarray, k: int, seed: int, model: str) -> np.ndarray:
    if model == "kmeans":
        return KMeans(n_clusters=k, n_init=10, random_state=seed).fit_predict(X)
    return GaussianMixture(n_components=k, covariance_type="full", n_init=2,
                           random_state=seed).fit_predict(X)


def q3_q4_sweep(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Sweep k for both feature blocks and score each by stability.

    Stability is bootstrap agreement: refit on two independent resampled halves
    and score the labels the two fits assign to the *same* held-in points with
    adjusted Rand. A k that does not survive resampling is an artifact of k.

    The sweep uses k-means because the question here is only *how many families
    and on which features* -- a question about the geometry, which k-means reads
    cheaply enough to bootstrap. GMM is scored once per k for BIC (soft
    membership is the point of GMM and matters at Level 3, not here), without
    bootstrap refits.
    """
    rows = []
    for block_name, cols in (("movement", BLOCK_MOVEMENT), ("movement+axis", BLOCK_WITH_AXIS)):
        idx = rng.choice(len(df), size=N_FIT, replace=False)
        X = StandardScaler().fit_transform(df.iloc[idx][cols].to_numpy(dtype="float64"))
        sil_idx = rng.choice(len(X), size=N_SIL, replace=False)

        for k in K_GRID:
            labels = _fit_labels(X, k, SEED, "kmeans")
            sil = silhouette_score(X[sil_idx], labels[sil_idx])

            g = GaussianMixture(n_components=k, covariance_type="full",
                                n_init=1, random_state=SEED).fit(X)
            bic = g.bic(X)

            aris = []
            for b in range(N_BOOT):
                r = np.random.default_rng(SEED + b)
                a_idx = r.choice(len(X), size=len(X) // 2, replace=False)
                b_idx = r.choice(len(X), size=len(X) // 2, replace=False)
                overlap = np.intersect1d(a_idx, b_idx)
                if len(overlap) < 500:
                    continue
                la = _fit_labels(X[a_idx], k, SEED + b, "kmeans")
                lb = _fit_labels(X[b_idx], k, SEED + b, "kmeans")
                pos_a = {v: i for i, v in enumerate(a_idx)}
                pos_b = {v: i for i, v in enumerate(b_idx)}
                aris.append(adjusted_rand_score(
                    la[[pos_a[o] for o in overlap]],
                    lb[[pos_b[o] for o in overlap]],
                ))
            rows.append({
                "block": block_name, "k": k,
                "silhouette": round(float(sil), 4),
                "gmm_bic": round(float(bic), 1),
                "stability_ari": round(float(np.mean(aris)), 4),
                "stability_ari_sd": round(float(np.std(aris)), 4),
            })
            print(f"  {block_name:14s} k={k:2d} sil={sil:.3f} "
                  f"ari={np.mean(aris):.3f} bic={bic:,.0f}", flush=True)
    return pd.DataFrame(rows)


def crosstab_vs_labels(df: pd.DataFrame, rng: np.random.Generator, k: int,
                       block: list[str], model: str) -> pd.DataFrame:
    idx = rng.choice(len(df), size=N_FIT, replace=False)
    d = df.iloc[idx].copy()
    X = StandardScaler().fit_transform(d[block].to_numpy(dtype="float64"))
    d["family"] = _fit_labels(X, k, SEED, model)

    ct = pd.crosstab(d["family"], d["pitch_type"])
    share = ct.div(ct.sum(axis=1), axis=0)
    summary = d.groupby("family")[block].mean().round(2)
    summary["n"] = ct.sum(axis=1)
    summary["purity"] = share.max(axis=1).round(3)
    summary["top_type"] = share.idxmax(axis=1)
    summary["second_type"] = share.apply(
        lambda r: r.drop(r.idxmax()).idxmax(), axis=1)
    summary["second_share"] = share.apply(
        lambda r: r.drop(r.idxmax()).max(), axis=1).round(3)
    return summary.reset_index()


def main() -> None:
    OUT.mkdir(exist_ok=True)
    rng = np.random.default_rng(SEED)
    df = load()
    print(f"loaded {len(df):,} pitches, {df['pitcher'].nunique()} pitchers\n")

    print("Q1 -- handedness mirroring audit")
    q1 = q1_mirroring(df, rng)
    print(q1.to_string(index=False), "\n")
    q1.to_csv(OUT / "l1_q1_mirroring_audit.csv", index=False)

    print("Q2 -- PCA of mirrored shape space")
    var, loadings = q2_pca(df, rng)
    print(var.to_string(index=False))
    print(loadings.to_string(index=False), "\n")
    var.to_csv(OUT / "l1_q2_pca_variance.csv", index=False)
    loadings.to_csv(OUT / "l1_q2_pca_loadings.csv", index=False)

    print("Q3/Q4 -- k sweep, both feature blocks, both models")
    sweep = q3_q4_sweep(df, rng)
    sweep.to_csv(OUT / "l1_q3_k_sweep.csv", index=False)

    best = sweep.sort_values("stability_ari", ascending=False).iloc[0]
    print(f"\nmost stable configuration: {best['block']} / k={int(best['k'])} "
          f"(ARI {best['stability_ari']})")

    block = BLOCK_MOVEMENT if best["block"] == "movement" else BLOCK_WITH_AXIS
    ct = crosstab_vs_labels(df, rng, int(best["k"]), block, "kmeans")
    print("\nQ4 -- families vs crowdsourced pitch_type")
    print(ct.to_string(index=False))
    ct.to_csv(OUT / "l1_q4_family_vs_pitchtype.csv", index=False)

    print(f"\noutputs -> {OUT}")


if __name__ == "__main__":
    main()
