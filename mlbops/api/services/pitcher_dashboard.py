"""Pitcher dashboard facts for Launch Station queue review.

This intentionally computes human-browsable views from warehouse pitch-level data
instead of only reformatting the card metadata JSON.
"""

from __future__ import annotations

import math
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from api.paths import get_warehouse_dir, safe_is_dir


_PITCH_RE = re.compile(r"^game_(\d+)_(\d{8})_pitches_enriched\.parquet$")
_STAGES = ("regular_season", "postseason", "spring_training")
_SWING = {
    "foul_bunt",
    "foul",
    "hit_into_play",
    "swinging_strike",
    "foul_tip",
    "swinging_strike_blocked",
    "missed_bunt",
    "bunt_foul_tip",
}
_WHIFF = {"swinging_strike", "foul_tip", "swinging_strike_blocked", "missed_bunt"}
_CSW = {"called_strike", "swinging_strike", "swinging_strike_blocked", "foul_tip"}
_HITS = {"single", "double", "triple", "home_run"}
_BB = {"walk", "intent_walk"}
_K = {"strikeout", "strikeout_double_play"}
_OUTS = {
    "strikeout",
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "double_play",
    "fielders_choice_out",
    "sac_fly",
    "sac_bunt",
    "strikeout_double_play",
    "other_out",
}
_GB = {"ground_ball"}
_FB = {"fly_ball", "popup"}
_FASTBALLS = {"FF", "SI", "FC", "FA"}
_COLS = [
    "pitcher",
    "pitcher_id",
    "pitch_type",
    "game_date",
    "game_pk",
    "inning",
    "inning_topbot",
    "at_bat_number",
    "pitch_number",
    "description",
    "type",
    "events",
    "outs_when_up",
    "stand",
    "release_speed",
    "estimated_woba_using_speedangle",
    "launch_speed",
    "launch_angle",
    "bb_type",
    "home_score",
    "away_score",
    "home_team",
    "away_team",
]


def _norm_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .replace({"nan": pd.NA, "none": pd.NA, "": pd.NA})
    )


def _file_game(path: Path) -> tuple[int | None, str | None]:
    m = _PITCH_RE.match(path.name)
    if not m:
        return None, None
    try:
        return int(m.group(1)), datetime.strptime(m.group(2), "%Y%m%d").date().isoformat()
    except ValueError:
        return None, None


def _season_files(warehouse: Path, season: int, through_date: str) -> list[Path]:
    through_ymd = through_date.replace("-", "")[:8]
    out: list[Path] = []
    for stage in _STAGES:
        root = warehouse / str(season) / stage / "pitches_enriched"
        if not safe_is_dir(root):
            continue
        for path in root.glob("game_*_pitches_enriched.parquet"):
            m = _PITCH_RE.match(path.name)
            if not m:
                continue
            if m.group(2) <= through_ymd:
                out.append(path)
    return sorted(out)


def _read_pitcher_file(path: Path, pitcher_id: int) -> pd.DataFrame | None:
    try:
        schema_cols = set(pq.ParquetFile(path).schema_arrow.names)
    except Exception:
        return None
    pcol = "pitcher" if "pitcher" in schema_cols else "pitcher_id" if "pitcher_id" in schema_cols else None
    if not pcol:
        return None
    cols = [c for c in _COLS if c in schema_cols]
    try:
        df = pd.read_parquet(path, columns=cols, filters=[(pcol, "=", int(pitcher_id))])
    except Exception:
        try:
            df = pd.read_parquet(path, columns=cols)
            df = df[df[pcol] == int(pitcher_id)]
        except Exception:
            return None
    if df.empty:
        return None
    gpk, gdate = _file_game(path)
    if "game_pk" not in df.columns or df["game_pk"].isna().all():
        df["game_pk"] = gpk
    if "game_date" not in df.columns or df["game_date"].isna().all():
        df["game_date"] = gdate
    else:
        df["game_date"] = df["game_date"].astype(str).str.slice(0, 10)
    return df


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["description_norm"] = _norm_series(out["description"]) if "description" in out.columns else pd.NA
    out["events_norm"] = _norm_series(out["events"]) if "events" in out.columns else pd.NA
    if "pitch_type" not in out.columns:
        out["pitch_type"] = "UN"
    out["pitch_type"] = out["pitch_type"].fillna("UN").astype(str)
    out["is_swing"] = out["description_norm"].isin(_SWING)
    out["is_whiff"] = out["description_norm"].isin(_WHIFF)
    out["is_csw"] = out["description_norm"].isin(_CSW)
    out["is_k"] = out["events_norm"].isin(_K)
    out["is_bb"] = out["events_norm"].isin(_BB)
    out["is_hit"] = out["events_norm"].isin(_HITS)
    out["is_hr"] = out["events_norm"].eq("home_run")
    out["is_out"] = out["events_norm"].isin(_OUTS)
    out["is_bip"] = out["description_norm"].eq("hit_into_play")
    out["is_gb"] = _norm_series(out["bb_type"]).isin(_GB) if "bb_type" in out.columns else False
    out["is_fb"] = _norm_series(out["bb_type"]).isin(_FB) if "bb_type" in out.columns else False
    typ = out["type"].astype(str).str.strip().str.upper() if "type" in out.columns else pd.Series("", index=out.index)
    out["is_strike"] = typ.isin({"S", "X"})
    out["is_ball"] = typ.eq("B")
    topbot = out["inning_topbot"].astype(str).str.lower() if "inning_topbot" in out.columns else pd.Series("", index=out.index)
    out["home_road"] = topbot.str.startswith("top").map({True: "Home", False: "Road"}).fillna("—")
    out["month"] = pd.to_datetime(out["game_date"], errors="coerce").dt.strftime("%b %Y")
    return out


def _last_pa_rows(df: pd.DataFrame) -> pd.DataFrame:
    group_cols = [c for c in ("game_pk", "inning", "at_bat_number") if c in df.columns]
    sort_cols = [c for c in ("game_pk", "inning", "at_bat_number", "pitch_number") if c in df.columns]
    if len(group_cols) >= 3 and len(sort_cols) >= 3:
        return df.sort_values(sort_cols).groupby(group_cols, dropna=False).tail(1)
    return df[df["events_norm"].notna()].copy()


def _outs_to_ip(outs: int) -> str:
    return f"{outs // 3}.{outs % 3}"


def _event_outs(event: Any) -> int:
    e = str(event or "").strip().lower().replace(" ", "_").replace("-", "_")
    if e == "grounded_into_double_play":
        return 2
    if e in {"triple_play"}:
        return 3
    return 1 if e in _OUTS else 0


def _outs_from_pa_rows(pa: pd.DataFrame) -> int:
    if pa.empty:
        return 0
    fallback = int(pa["events_norm"].map(_event_outs).sum()) if "events_norm" in pa.columns else 0
    if "outs_when_up" not in pa.columns:
        return fallback

    sort_cols = [c for c in ("game_pk", "inning", "at_bat_number", "pitch_number") if c in pa.columns]
    ordered = pa.sort_values(sort_cols).copy() if sort_cols else pa.copy()
    ordered["_outs_before"] = pd.to_numeric(ordered["outs_when_up"], errors="coerce")
    if ordered["_outs_before"].isna().all():
        return fallback

    total = 0
    group_cols = [c for c in ("game_pk", "inning") if c in ordered.columns]
    groups = ordered.groupby(group_cols, dropna=False, sort=False) if group_cols else [(None, ordered)]
    for _, inning_pa in groups:
        inning_pa = inning_pa.reset_index(drop=True)
        before = inning_pa["_outs_before"].fillna(0).clip(lower=0, upper=2).astype(int)
        for i, row in inning_pa.iterrows():
            if i + 1 < len(inning_pa):
                after = int(before.iloc[i + 1])
            else:
                event_outs = _event_outs(row.get("events_norm"))
                after = min(3, int(before.iloc[i]) + event_outs)
            total += max(0, after - int(before.iloc[i]))
    return int(total)


def _safe_rate(num: Any, den: Any) -> float | None:
    try:
        n = float(num)
        d = float(den)
        if d <= 0 or not math.isfinite(n) or not math.isfinite(d):
            return None
        return n / d
    except Exception:
        return None


def _round(v: Any, digits: int = 3) -> float | None:
    try:
        f = float(v)
        if not math.isfinite(f):
            return None
        return round(f, digits)
    except Exception:
        return None


def _line_from_group(g: pd.DataFrame) -> dict[str, Any]:
    pa = _last_pa_rows(g)
    outs = _outs_from_pa_rows(pa)
    tbf = len(pa)
    pitches = len(g)
    xw = pd.to_numeric(pa.get("estimated_woba_using_speedangle"), errors="coerce")
    return {
        "pitches": pitches,
        "tbf": tbf,
        "ip": _outs_to_ip(outs),
        "outs": outs,
        "hits": int(pa["is_hit"].sum()),
        "k": int(pa["is_k"].sum()),
        "bb": int(pa["is_bb"].sum()),
        "hr": int(pa["is_hr"].sum()),
        "whiffs": int(g["is_whiff"].sum()),
        "swstr_pct": _round(_safe_rate(g["is_whiff"].sum(), pitches) * 100 if pitches else None, 1),
        "csw_pct": _round(_safe_rate(g["is_csw"].sum(), pitches) * 100 if pitches else None, 1),
        "strike_pct": _round(_safe_rate(g["is_strike"].sum(), pitches) * 100 if pitches else None, 1),
        "ball_pct": _round(_safe_rate(g["is_ball"].sum(), pitches) * 100 if pitches else None, 1),
        "k_pct": _round(_safe_rate(pa["is_k"].sum(), tbf) * 100 if tbf else None, 1),
        "bb_pct": _round(_safe_rate(pa["is_bb"].sum(), tbf) * 100 if tbf else None, 1),
        "kbb_pct": _round(_safe_rate(pa["is_k"].sum() - pa["is_bb"].sum(), tbf) * 100 if tbf else None, 1),
        "xwoba": _round(xw.mean(), 3),
        "gb_pct": _round(_safe_rate(g["is_gb"].sum(), g["is_bip"].sum()) * 100 if g["is_bip"].sum() else None, 1),
        "fb_pct": _round(_safe_rate(g["is_fb"].sum(), g["is_bip"].sum()) * 100 if g["is_bip"].sum() else None, 1),
        "hard_pct": _round(_safe_rate((pd.to_numeric(g.get("launch_speed"), errors="coerce") >= 95).sum(), g["is_bip"].sum()) * 100 if g["is_bip"].sum() else None, 1),
    }


def _season_pitch_table(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    total = max(len(df), 1)
    for pt, g in df.groupby("pitch_type", dropna=False):
        line = _line_from_group(g)
        pa = _last_pa_rows(g)
        rows.append({
            "pitch_type": str(pt),
            "pitches": int(line["pitches"]),
            "use_pct": _round(line["pitches"] / total * 100, 1),
            "avg_velo": _round(pd.to_numeric(g.get("release_speed"), errors="coerce").mean(), 1),
            "swstr_pct": line["swstr_pct"],
            "strike_pct": line["strike_pct"],
            "ball_pct": line["ball_pct"],
            "gb_pct": line["gb_pct"],
            "fb_pct": line["fb_pct"],
            "hr": int(pa["is_hr"].sum()),
            "xwoba": line["xwoba"],
        })
    rows.sort(key=lambda r: r["pitches"], reverse=True)
    return rows


def _split_rows(df: pd.DataFrame, key: str) -> list[dict[str, Any]]:
    rows = []
    for label, g in df.groupby(key, dropna=False):
        line = _line_from_group(g)
        rows.append({"label": str(label), **line})
    return rows


def _game_logs(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for (gdate, gpk), g in df.groupby(["game_date", "game_pk"], dropna=False):
        line = _line_from_group(g)
        counts = g["pitch_type"].value_counts()
        top = counts.head(4)
        total = max(int(counts.sum()), 1)
        ff = g[g["pitch_type"].isin(_FASTBALLS)]
        rows.append({
            "game_date": str(gdate)[:10],
            "game_pk": int(gpk) if pd.notna(gpk) else None,
            **line,
            "ff_velo": _round(pd.to_numeric(ff.get("release_speed"), errors="coerce").mean(), 1) if not ff.empty else None,
            "pitch_mix": [
                {"pitch_type": str(pt), "use_pct": _round(cnt / total * 100, 1)}
                for pt, cnt in top.items()
            ],
        })
    rows.sort(key=lambda r: (r["game_date"], r.get("game_pk") or 0))
    return rows


def _pitch_mix_trend(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trend = []
    for row in logs[-10:]:
        base = {"game_date": row["game_date"], "game_pk": row.get("game_pk")}
        for mix in row.get("pitch_mix") or []:
            base[str(mix["pitch_type"])] = mix["use_pct"]
        trend.append(base)
    return trend


def _inning_damage(df: pd.DataFrame, game_pk: int | None) -> list[dict[str, Any]]:
    g = df[df["game_pk"] == game_pk] if game_pk is not None and "game_pk" in df.columns else df.tail(0)
    if g.empty or "inning" not in g.columns:
        return []
    rows = []
    for inning, sub in g.groupby("inning", dropna=False):
        pa = _last_pa_rows(sub)
        runs = None
        if {"home_score", "away_score", "inning_topbot"}.issubset(sub.columns):
            try:
                t = sub.sort_values([c for c in ("inning", "at_bat_number", "pitch_number") if c in sub.columns])
                top = t["inning_topbot"].astype(str).str.lower().str.startswith("top")
                opp_score = pd.Series(
                    [a if is_top else h for is_top, a, h in zip(top, t["away_score"], t["home_score"])],
                    index=t.index,
                ).astype(float)
                runs = max(0, int(opp_score.iloc[-1] - opp_score.iloc[0]))
            except Exception:
                runs = None
        rows.append({
            "inning": int(inning) if pd.notna(inning) else None,
            "hits": int(pa["is_hit"].sum()),
            "hr": int(pa["is_hr"].sum()),
            "k": int(pa["is_k"].sum()),
            "bb": int(pa["is_bb"].sum()),
            "runs": runs,
            "events": [str(e) for e in pa["events_norm"].dropna().tolist() if str(e) in _HITS or str(e) in _K or str(e) in _BB][:8],
        })
    rows.sort(key=lambda r: r["inning"] or 0)
    return rows


def build_pitcher_dashboard(item: dict[str, Any]) -> dict[str, Any]:
    meta = {}
    try:
        json_meta = item.get("meta_json")
        meta = json.loads(json_meta) if json_meta else {}
    except Exception:
        meta = {}
    if item.get("content_type") != "pitcher_card" and meta.get("card_type") != "pitcher_card":
        raise ValueError("Pitcher dashboard is only available for pitcher_card queue items.")
    pitcher_id = int(item.get("player_id") or meta.get("pitcher_id") or 0)
    game_date = str(item.get("game_date") or meta.get("game_date") or "")[:10]
    if not pitcher_id or not game_date:
        raise ValueError("Queue item is missing pitcher_id or game_date.")
    season = int(item.get("season") or game_date[:4])
    game_pk = item.get("game_pk") or meta.get("game_pk")
    game_pk = int(game_pk) if game_pk is not None else None

    warehouse = get_warehouse_dir()
    files = _season_files(warehouse, season, game_date)
    frames = []
    for path in files:
        frame = _read_pitcher_file(path, pitcher_id)
        if frame is not None and not frame.empty:
            frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"No warehouse pitches found for pitcher {pitcher_id} through {game_date}.")
    df = _prepare(pd.concat(frames, ignore_index=True))
    logs = _game_logs(df)
    selected = next((r for r in logs if r.get("game_pk") == game_pk), logs[-1] if logs else {})
    summary = _line_from_group(df)
    pitch_table = _season_pitch_table(df)
    pitch_counts = Counter(df["pitch_type"].dropna().astype(str))

    return {
        "player_id": pitcher_id,
        "player_name": item.get("player_name") or meta.get("player_name"),
        "team": meta.get("team"),
        "opponent": item.get("opponent") or meta.get("opponent"),
        "game_date": game_date,
        "game_pk": game_pk,
        "warehouse_dir": str(warehouse),
        "sample": {"games": len(logs), "pitches": int(len(df)), "through": game_date},
        "season_summary": summary,
        "selected_game": selected,
        "month_splits": _split_rows(df, "month"),
        "batter_hand_splits": _split_rows(df, "stand") if "stand" in df.columns else [],
        "home_road_splits": _split_rows(df, "home_road"),
        "pitch_table": pitch_table,
        "game_logs": logs[-12:],
        "pitch_mix_trend": _pitch_mix_trend(logs),
        "fastball_velo_trend": [
            {"game_date": r["game_date"], "game_pk": r.get("game_pk"), "ff_velo": r.get("ff_velo")}
            for r in logs[-12:]
            if r.get("ff_velo") is not None
        ],
        "pitch_count_trend": [
            {"game_date": r["game_date"], "game_pk": r.get("game_pk"), "pitches": r.get("pitches")}
            for r in logs[-12:]
        ],
        "k_trend": [
            {"game_date": r["game_date"], "game_pk": r.get("game_pk"), "k": r.get("k"), "whiffs": r.get("whiffs")}
            for r in logs[-12:]
        ],
        "inning_damage": _inning_damage(df, game_pk),
        "top_pitch_types": [pt for pt, _ in pitch_counts.most_common(5)],
        "limitations": [
            "Computed from local pitches_enriched warehouse files.",
            "Official proprietary metrics in the reference dashboard, such as JA SIERA or Stuff+, are not inferred.",
        ],
    }
