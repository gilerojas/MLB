"""Phase 2b -- does the shape space cluster more than chance, and at what k?

Phase 2 produced a bootstrap stability curve that decays monotonically in k, and
picking its maximum would have selected k=4. That is a property of adjusted
Rand, not of baseball: fewer clusters are mechanically easier to reproduce
across resamples, so raw ARI always favours small k and can never identify an
interior structure.

The fix is a null. For each k, the identical pipeline runs twice:

    real   the mirrored shape matrix as observed
    null   each feature column shuffled independently, which preserves every
           marginal distribution exactly and destroys only the joint structure

The null still clusters -- k-means always returns k clusters, and a product of
four realistic marginals is still stably partitionable. What the null cannot
have is *structure*, so `excess = ARI_real - ARI_null` is the part of stability
attributable to real joint geometry. The k that maximises excess is the number
of families the data actually supports.

    ./mlb_env.nosync/bin/python research/study/pitching_archetypes/stability_vs_null.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.study.pitching_archetypes.explore_shape_space import (  # noqa: E402
    BLOCK_MOVEMENT, K_GRID, N_FIT, load,
)
from research.study.pitching_archetypes.features import SEED  # noqa: E402

OUT = Path(__file__).resolve().parent / "outputs"
N_BOOT = 6


def bootstrap_ari(X: np.ndarray, k: int, n_boot: int = N_BOOT) -> tuple[float, float]:
    aris = []
    for b in range(n_boot):
        r = np.random.default_rng(SEED + 1000 + b)
        a_idx = r.choice(len(X), size=len(X) // 2, replace=False)
        b_idx = r.choice(len(X), size=len(X) // 2, replace=False)
        overlap = np.intersect1d(a_idx, b_idx)
        la = KMeans(k, n_init=5, random_state=SEED + b).fit_predict(X[a_idx])
        lb = KMeans(k, n_init=5, random_state=SEED + b).fit_predict(X[b_idx])
        pos_a = {v: i for i, v in enumerate(a_idx)}
        pos_b = {v: i for i, v in enumerate(b_idx)}
        aris.append(adjusted_rand_score(
            la[[pos_a[o] for o in overlap]], lb[[pos_b[o] for o in overlap]]))
    return float(np.mean(aris)), float(np.std(aris))


def main() -> None:
    OUT.mkdir(exist_ok=True)
    rng = np.random.default_rng(SEED)
    df = load()

    idx = rng.choice(len(df), size=N_FIT, replace=False)
    X = StandardScaler().fit_transform(df.iloc[idx][BLOCK_MOVEMENT].to_numpy(dtype="float64"))

    # Column-independent shuffle: identical marginals, no joint structure.
    Xn = X.copy()
    for j in range(Xn.shape[1]):
        Xn[:, j] = rng.permutation(Xn[:, j])

    rows = []
    for k in K_GRID:
        real, real_sd = bootstrap_ari(X, k)
        null, null_sd = bootstrap_ari(Xn, k)
        rows.append({
            "k": k,
            "ari_real": round(real, 4), "ari_real_sd": round(real_sd, 4),
            "ari_null": round(null, 4), "ari_null_sd": round(null_sd, 4),
            "excess": round(real - null, 4),
        })
        print(f"  k={k:2d}  real={real:.3f}  null={null:.3f}  excess={real-null:+.3f}",
              flush=True)

    res = pd.DataFrame(rows)
    res.to_csv(OUT / "l1_q3b_stability_vs_null.csv", index=False)

    best = res.sort_values("excess", ascending=False).iloc[0]
    print(f"\nmax excess stability at k={int(best['k'])} "
          f"(real {best['ari_real']} vs null {best['ari_null']}, "
          f"excess {best['excess']:+.3f})")
    print(f"outputs -> {OUT}")


if __name__ == "__main__":
    main()
