import os
from jinja2 import Environment, FileSystemLoader, select_autoescape
import databaseConnector
from datetime import datetime, timezone
import argparse
import math
from contextlib import closing
from pageGeneration import generateSpecNav, ROLE_FOLDERS
from aggregateData import get_current_season_id, get_access_token
from generateSpecPages import (
    LOOKUP_DIR,
    humanize_number,
    format_duration,
    format_utc_timestamp,
    upgrade_info,
    load_json,
)

# config
CLIENT_ID = os.environ["BLIZ_CLIENT_ID"]
CLIENT_SECRET = os.environ["BLIZ_CLIENT_SECRET"]

databaseConnector.init_connection_pool(
    os.environ.get("DATABASE_HOST"),
    os.environ.get("DATABASE_USER"),
    os.environ.get("DATABASE_PASSWORD"),
    os.environ.get("DATABASE_NAME"),
    os.environ.get("DATABASE_PORT"),
    1,
)


def _finish_building_tiers_from_items(items, k=6):
    tier_letters = ["S", "A", "B", "C", "D", "F"]
    n = len(items)
    if n == 0:
        return {L: [] for L in tier_letters}

    # sort items by score desc (lb_ci)
    items.sort(key=lambda it: it["lb_ci"], reverse=True)

    if n < len(tier_letters):
        out = {L: [] for L in tier_letters}
        for i, it in enumerate(items):
            out[tier_letters[i]].append(it)
        for L in out:
            for it in out[L]:
                it["score"] = it["lb_ci"]
        return out

    # ckmeans on the lb_ci values
    labels = ckmeans_1d([it["lb_ci"] for it in items], k=min(k, n))

    # compute cluster means
    cluster_sums = {}
    cluster_counts = {}
    for lab, it in zip(labels, items):
        v = it["lb_ci"]
        cluster_sums[lab] = cluster_sums.get(lab, 0.0) + v
        cluster_counts[lab] = cluster_counts.get(lab, 0) + 1
    cluster_means = {
        lab: (cluster_sums[lab] / cluster_counts[lab]) for lab in cluster_sums
    }

    # order clusters by descending mean and map to tier letters
    ordered_clusters = sorted(
        cluster_means.keys(), key=lambda L: cluster_means[L], reverse=True
    )
    cluster_to_tier = {}
    for i, cid in enumerate(ordered_clusters):
        if i < len(tier_letters):
            cluster_to_tier[cid] = tier_letters[i]
        else:
            cluster_to_tier[cid] = tier_letters[-1]

    tiers = {L: [] for L in tier_letters}
    for it, lab in zip(items, labels):
        tier = cluster_to_tier.get(lab, tier_letters[-1])
        it["score"] = it["lb_ci"]
        tiers[tier].append(it)

    for L in tiers:
        tiers[L] = sorted(tiers[L], key=lambda d: d["score"], reverse=True)

    # repair function (same deterministic repair as before)
    def repair_tiers(tiers_dict):
        letters = tier_letters
        k_len = len(letters)
        for L in letters:
            tiers_dict[L] = sorted(
                tiers_dict.get(L, []), key=lambda d: d["score"], reverse=True
            )

        for i, L in enumerate(letters):
            if len(tiers_dict[L]) == 0:
                moved = False
                for j in range(i + 1, k_len):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 1:
                        item = tiers_dict[Lj].pop(0)
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                for j in range(i - 1, -1, -1):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 1:
                        item = tiers_dict[Lj].pop(-1)
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                for j in range(i + 1, k_len):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 0:
                        item = tiers_dict[Lj].pop(0)
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                for j in range(i - 1, -1, -1):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 0:
                        item = tiers_dict[Lj].pop(-1)
                        tiers_dict[L].append(item)
                        moved = True
                        break
        for L in letters:
            tiers_dict[L] = sorted(
                tiers_dict.get(L, []), key=lambda d: d["score"], reverse=True
            )
        return tiers_dict

    repaired = repair_tiers({L: list(tiers[L]) for L in tier_letters})
    if all(len(repaired[L]) > 0 for L in tier_letters):
        return repaired

    # fallback: even-ranked split
    base = n // len(tier_letters)
    rem = n % len(tier_letters)
    out = {L: [] for L in tier_letters}
    idx = 0
    for i, L in enumerate(tier_letters):
        take = base + (1 if i < rem else 0)
        if take > 0:
            slice_items = items[idx : idx + take]
            for it in slice_items:
                it["score"] = it["lb_ci"]
            out[L].extend(slice_items)
            idx += take
    for L in out:
        out[L] = sorted(out[L], key=lambda d: d["score"], reverse=True)
    return out


# --------------------------
# 1D ckmeans (optimal 1D k-means) implementation
# Returns cluster index (0..k-1) for each input value (same order as input).
# Complexity: O(n^2 * k) worst-case which is fine for n ~ number of dungeons.
# --------------------------
def ckmeans_1d(values, k):
    """
    values: list of floats
    k: number of clusters
    returns: list of cluster indices for each value (same order as input)
    """
    n = len(values)
    if n == 0:
        return []
    if k <= 1:
        return [0] * n
    # sort values with original indices
    sorted_pairs = sorted(enumerate(values), key=lambda iv: iv[1])
    idx_sorted = [p[0] for p in sorted_pairs]
    x = [p[1] for p in sorted_pairs]

    # prefix sums (1-based for convenience)
    S1 = [0.0] * (n + 1)  # sum x
    S2 = [0.0] * (n + 1)  # sum x^2
    for i in range(1, n + 1):
        S1[i] = S1[i - 1] + x[i - 1]
        S2[i] = S2[i - 1] + x[i - 1] * x[i - 1]

    def sq_err(i, j):
        # squared error for segment x[i..j] inclusive, 0-based indices
        # using prefix sums S1,S2 where S1 index is +1
        m = j - i + 1
        s1 = S1[j + 1] - S1[i]
        s2 = S2[j + 1] - S2[i]
        # SSE = s2 - (s1^2 / m)
        res = s2 - (s1 * s1) / m
        # numerical safety
        return max(0.0, res)

    # dp[k+1][n+1] large
    INF = float("inf")
    dp = [[INF] * (n + 1) for _ in range(k + 1)]
    back = [[-1] * (n + 1) for _ in range(k + 1)]
    dp[0][0] = 0.0

    # dynamic programming
    for clusters in range(1, k + 1):
        # need at least clusters points to form clusters
        for j in range(clusters, n + 1):
            # partition previous cluster ending at i-1, current cluster i..j-1 (1-based indexing in dp)
            best_cost = INF
            best_i = -1
            # iterate possible split points i
            # optimize by limiting range? keep simple for clarity
            for i in range(clusters - 1, j):
                cost = dp[clusters - 1][i] + sq_err(i, j - 1)
                if cost < best_cost:
                    best_cost = cost
                    best_i = i
            dp[clusters][j] = best_cost
            back[clusters][j] = best_i

    # backtrack clusters boundaries
    clusters = k
    j = n
    boundaries = []
    while clusters > 0:
        i = back[clusters][j]
        boundaries.append((i, j - 1))  # indices in sorted x
        j = i
        clusters -= 1
    boundaries.reverse()  # from low to high

    # assign labels per sorted index
    labels_sorted = [None] * n
    label = 0
    for start, end in boundaries:
        for t in range(start, end + 1):
            labels_sorted[t] = label
        label += 1

    # map back to original order
    labels = [None] * n
    for sorted_pos, orig_idx in enumerate(idx_sorted):
        labels[orig_idx] = labels_sorted[sorted_pos]
    return labels


# --------------------------
# compute LB_CI from aggregated counts using exponential weighting by level
# --------------------------
def compute_weighted_stats_and_lbci(
    rows,
    id_key="dungeon_id",
    keystone_key="keystone_level",
    total_runs_key="total_runs",
    value_keys=("upgrade_3", "upgrade_2", "upgrade_1", "depleted"),
    value_weights=(3, 2, 1, 0),
    weight_base=1.6,
    ci_z=1.96,
):
    """
    Generic aggregator that computes weighted mean, variance and LB_CI per id_key.

    rows: iterable of dict-like records
    id_key: field name that identifies the item (e.g. "dungeon_id" or "spec_id")
    keystone_key: field name for the key level (defaults to "keystone_level")
    total_runs_key: field name for total runs in that row
    value_keys: ordered tuple of keys corresponding to value_weights
                e.g. ("upgrade_3","upgrade_2","upgrade_1","depleted")
    value_weights: numeric weights corresponding to the above (default 3,2,1,0)
    weight_base: exponential base to emphasize higher keys
    ci_z: z-score for confidence interval (1.96 for 95%)

    Returns: dict mapping id -> { N, mean, var, lb_ci, sum counts... }
    """
    # find global min level for stable relative weighting
    min_level = None
    for r in rows:
        L = r.get(keystone_key)
        try:
            L = int(L or 0)
        except Exception:
            L = 0
        if min_level is None or L < min_level:
            min_level = L
    if min_level is None:
        min_level = 0

    # re-iterate rows (we need to allow rows to be an iterator; so better to convert to list)
    # If rows is a generator this makes a list; that's fine for typical sizes (specs/dungeons).
    rows_list = list(rows)

    by_id = {}
    for r in rows_list:
        raw_id = r.get(id_key)
        if raw_id is None:
            continue
        # try to cast to int if it looks numeric, else keep as-is (string)
        try:
            item_id = int(raw_id)
        except Exception:
            item_id = raw_id

        # keystone level and exponential weight
        try:
            L = int(r.get(keystone_key, 0) or 0)
        except Exception:
            L = 0
        w = float(weight_base) ** (L - min_level)

        # collect counts according to provided value keys
        counts = []
        for k in value_keys:
            try:
                counts.append(int(r.get(k, 0) or 0))
            except Exception:
                counts.append(0)

        try:
            tr = int(r.get(total_runs_key, 0) or 0)
        except Exception:
            tr = 0
        if tr <= 0:
            # skip rows with zero runs
            continue

        # weighted sum of per-run values, sum of squares
        # per-run value = sum(count_i * weight_value_i)
        per_run_value_sum = 0
        per_run_value_sq_sum = 0
        # compute contributions for each category
        for cnt, val_w in zip(counts, value_weights):
            # each of cnt runs contributed 'val_w' value
            per_run_value_sum += cnt * val_w
            per_run_value_sq_sum += cnt * (val_w * val_w)

        # apply weighting by w to sums (note: sum over rows is weighted by w, and N uses total_runs)
        sum_val = w * per_run_value_sum
        sum_sq = (w * w) * per_run_value_sq_sum

        entry = by_id.setdefault(
            item_id,
            {
                "N": 0,
                "sum_wv": 0.0,
                "sum_wv2": 0.0,
                # store the raw counts for downstream display
                **{k: 0 for k in value_keys},
                total_runs_key: 0,
            },
        )

        entry["N"] += tr
        entry["sum_wv"] += sum_val
        entry["sum_wv2"] += sum_sq
        for k, cnt in zip(value_keys, counts):
            entry[k] = entry.get(k, 0) + cnt
        entry[total_runs_key] = entry.get(total_runs_key, 0) + tr

    # compute mean, var, lb_ci per item
    out = {}
    for item_id, e in by_id.items():
        N = e["N"]
        if N <= 0:
            mean = 0.0
            var = 0.0
            lb = 0.0
        else:
            mean = e["sum_wv"] / N
            E_x2 = e["sum_wv2"] / N
            var = E_x2 - (mean * mean)
            var = max(0.0, var)
            se = math.sqrt(var / N) if N > 0 else 0.0
            lb = mean - ci_z * se
        # compact output (include original counts and total_runs)
        out[item_id] = {
            "N": N,
            "mean": mean,
            "var": var,
            "lb_ci": lb,
            **{k: e.get(k, 0) for k in value_keys},
            "total_runs": e.get(total_runs_key, 0),
        }
    return out


# --------------------------
# build tiers using LB_CI + ckmeans clustering
# --------------------------
def build_ckmeans_tiers(dungeon_lookup, runs_rows, weight_base=1.6, k=6):
    """
    Returns tiers dict mapping tier_letter -> list of dungeon dicts
    dungeon_lookup: mapping id -> metadata (name, icon, short)
    runs_rows: list of aggregated rows (as returned by your DB function)
    """
    stats = compute_weighted_stats_and_lbci(
        rows=runs_rows,
        id_key="dungeon_id",
        weight_base=weight_base,
    )
    # create list of dungeons with lb_ci values
    items = []
    for did, s in stats.items():
        meta = dungeon_lookup.get(str(did)) or dungeon_lookup.get(did) or {}
        items.append(
            {
                "dungeon_id": did,
                "name": meta.get("name", f"Dungeon {did}"),
                "short": meta.get("short", ""),
                "icon": meta.get("icon", None),
                "lb_ci": s["lb_ci"],
                "mean": s["mean"],
                "var": s["var"],
                "total_runs": s.get("total_runs", 0),
                "upgrade_3": s.get("upgrade_3", 0),
                "upgrade_2": s.get("upgrade_2", 0),
                "upgrade_1": s.get("upgrade_1", 0),
                "depleted": s.get("depleted", 0),
                "N": s["N"],
            }
        )

    # if no items, return empty structure
    n = len(items)
    tier_letters = ["S", "A", "B", "C", "D", "F"]
    k_target = k if k is not None else len(tier_letters)
    if n == 0:
        return {L: [] for L in tier_letters}

    # helper: sort items by score desc
    items.sort(key=lambda it: it["lb_ci"], reverse=True)

    # If fewer items than tiers, give top-most tiers one item each (deterministic)
    if n < len(tier_letters):
        out = {L: [] for L in tier_letters}
        for i, it in enumerate(items):
            out[tier_letters[i]].append(it)
        # add score field for template compatibility
        for L in out:
            for it in out[L]:
                it["score"] = it["lb_ci"]
        return out

    # Run ckmeans with up to n clusters
    labels = ckmeans_1d([it["lb_ci"] for it in items], k=min(k_target, n))

    # compute cluster means to order clusters
    cluster_sums = {}
    cluster_counts = {}
    for lab, it in zip(labels, items):
        v = it["lb_ci"]
        cluster_sums[lab] = cluster_sums.get(lab, 0.0) + v
        cluster_counts[lab] = cluster_counts.get(lab, 0) + 1
    cluster_means = {
        lab: (cluster_sums[lab] / cluster_counts[lab]) for lab in cluster_sums
    }

    # order cluster ids by descending mean (highest LB_CI -> top)
    ordered_clusters = sorted(
        cluster_means.keys(), key=lambda L: cluster_means[L], reverse=True
    )

    # Map available clusters to top-most tiers (deterministic)
    cluster_to_tier = {}
    for i, cid in enumerate(ordered_clusters):
        if i < len(tier_letters):
            cluster_to_tier[cid] = tier_letters[i]
        else:
            cluster_to_tier[cid] = tier_letters[-1]  # map extras to F

    # Build initial tiers (some tiers may be empty)
    tiers = {L: [] for L in tier_letters}
    for it, lab in zip(items, labels):
        tier = cluster_to_tier.get(lab, tier_letters[-1])
        it["score"] = it["lb_ci"]
        tiers[tier].append(it)

    # sort each tier by score desc
    for L in tiers:
        tiers[L] = sorted(tiers[L], key=lambda d: d["score"], reverse=True)

    # If all tiers are non-empty already, we're done
    if all(len(tiers[L]) > 0 for L in tier_letters):
        return tiers

    # Deterministic repair: ensure every tier has at least one item.
    # Strategy:
    # 1) For every empty tier (top->bottom), try to steal the top item from the nearest lower tier that has >1 items.
    # 2) If none, try to steal the bottom item from the nearest upper tier that has >1 items.
    # 3) If still impossible (rare), fallback to an even-ranked split across tiers (preserves global rank).
    def repair_tiers(tiers_dict):
        letters = tier_letters
        k_len = len(letters)
        # ensure sorted inside tiers
        for L in letters:
            tiers_dict[L] = sorted(
                tiers_dict.get(L, []), key=lambda d: d["score"], reverse=True
            )

        for i, L in enumerate(letters):
            if len(tiers_dict[L]) == 0:
                moved = False
                # look downward for donor with >1 items
                for j in range(i + 1, k_len):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 1:
                        item = tiers_dict[Lj].pop(0)  # top of lower tier
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                # look upward for donor with >1 items
                for j in range(i - 1, -1, -1):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 1:
                        item = tiers_dict[Lj].pop(-1)  # bottom of upper tier
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                # last resort: steal from nearest non-empty (may create another empty, but subsequent loop iterations will fix)
                for j in range(i + 1, k_len):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 0:
                        item = tiers_dict[Lj].pop(0)
                        tiers_dict[L].append(item)
                        moved = True
                        break
                if moved:
                    continue
                for j in range(i - 1, -1, -1):
                    Lj = letters[j]
                    if len(tiers_dict[Lj]) > 0:
                        item = tiers_dict[Lj].pop(-1)
                        tiers_dict[L].append(item)
                        moved = True
                        break
                # if still not moved, give up here (will be handled by fallback)
        # final sort
        for L in letters:
            tiers_dict[L] = sorted(
                tiers_dict.get(L, []), key=lambda d: d["score"], reverse=True
            )
        return tiers_dict

    repaired = repair_tiers({L: list(tiers[L]) for L in tier_letters})
    if all(len(repaired[L]) > 0 for L in tier_letters):
        return repaired

    # Fallback: even-ranked split (deterministic) to guarantee non-empty tiers
    # slice items (already sorted desc) into k buckets: top tiers get the larger remainders
    base = n // len(tier_letters)
    rem = n % len(tier_letters)
    out = {L: [] for L in tier_letters}
    idx = 0
    for i, L in enumerate(tier_letters):
        take = base + (1 if i < rem else 0)
        if take > 0:
            slice_items = items[idx : idx + take]
            for it in slice_items:
                it["score"] = it["lb_ci"]
            out[L].extend(slice_items)
            idx += take
    # final safety sort
    for L in out:
        out[L] = sorted(out[L], key=lambda d: d["score"], reverse=True)
    return out


def build_buff_tiers(buff_lookup, buff_rows, k=6):
    """
    Simple buff tiering based on reported percentage ('pct' field) or computed runs/total_runs.
    Accepts:
      - dict like {'total_runs': N, 'buffs': [{'id': id, 'runs': r, 'pct': p}, ...]}
      - or an iterable of rows where each row has 'buff_id' or 'id', 'runs' and optionally 'pct' and 'total_runs'

    We use pct (0..100) as the score (stored in 'lb_ci' for compatibility with the rest of the pipeline)
    and then delegate to the deterministic tier builder (_finish_building_tiers_from_items).
    """
    items = []
    # case: new compact dict form
    if isinstance(buff_rows, dict) and "buffs" in buff_rows:
        total_runs_global = int(buff_rows.get("total_runs", 0) or 0)
        for b in buff_rows.get("buffs", []):
            bid = b.get("id", None)
            if bid is None:
                continue
            runs = int(b.get("runs", 0) or 0)
            pct = b.get("pct")
            if pct is None:
                pct = (
                    (runs / total_runs_global * 100.0) if total_runs_global > 0 else 0.0
                )
            items.append(
                {
                    "buff_id": bid,
                    "name": (
                        buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}
                    ).get("name")
                    or (buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}).get(
                        "display_name"
                    )
                    or f"Buff {bid}",
                    "short": (
                        buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}
                    ).get("short", ""),
                    "icon": (
                        buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}
                    ).get("icon")
                    or (buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}).get(
                        "icon_file"
                    ),
                    # use pct (0..100) as the score field expected by downstream code (lb_ci)
                    "lb_ci": float(pct),
                    "mean": float(pct),
                    "var": 0.0,
                    "total_runs": total_runs_global,
                    "runs": runs,
                    "N": total_runs_global or runs,
                }
            )
    else:
        # assume iterable of rows (flexible shapes)
        for r in buff_rows or []:
            if r is None:
                continue
            bid = r.get("buff_id", r.get("id", None))
            if bid is None:
                continue
            runs = int(r.get("runs", 0) or 0)
            total_runs_row = int(r.get("total_runs", 0) or 0)
            pct = r.get("pct")
            if pct is None:
                pct = (runs / total_runs_row * 100.0) if total_runs_row > 0 else 0.0
            meta = buff_lookup.get(str(bid)) or buff_lookup.get(bid) or {}
            items.append(
                {
                    "buff_id": bid,
                    "name": meta.get("name")
                    or meta.get("display_name")
                    or f"Buff {bid}",
                    "short": meta.get("short", ""),
                    "icon": meta.get("icon") or meta.get("icon_file"),
                    "lb_ci": float(pct),
                    "mean": float(pct),
                    "var": 0.0,
                    "total_runs": total_runs_row or runs,
                    "runs": runs,
                    "N": total_runs_row or runs,
                }
            )

    # if nothing to do, return empty tier structure
    if not items:
        tier_letters = ["S", "A", "B", "C", "D", "F"]
        return {L: [] for L in tier_letters}

    # Sort by pct descending (keeps deterministic order for ckmeans / repair fallback)
    items.sort(key=lambda it: it["lb_ci"], reverse=True)

    # Reuse your deterministic tier builder which expects 'lb_ci' and returns tier-letter -> list
    return _finish_building_tiers_from_items(items, k)


def build_spec_tiers(spec_lookup, class_lookup, spec_rows, weight_base=1.6, k=6):
    # compute stats using generic aggregator keyed by spec_id
    stats = compute_weighted_stats_and_lbci(
        rows=spec_rows,
        id_key="spec_id",
        weight_base=weight_base,
    )
    items = []
    for sid, s in stats.items():
        meta = spec_lookup.get(str(sid)) or spec_lookup.get(sid) or {}
        class_id = meta.get("class_id") or meta.get("class") or None
        items.append(
            {
                "spec_id": sid,
                "name": meta.get("name", {"en_US": f"Spec {sid}"})
                if isinstance(meta.get("name"), dict)
                else meta.get("name", f"Spec {sid}"),
                "icon": meta.get("icon"),
                "class_id": class_id,
                "class_meta": class_lookup.get(str(class_id))
                or class_lookup.get(class_id)
                if class_lookup
                else None,
                "lb_ci": s["lb_ci"],
                "mean": s["mean"],
                "var": s["var"],
                "total_runs": s.get("total_runs", 0),
                "upgrade_3": s.get("upgrade_3", 0),
                "upgrade_2": s.get("upgrade_2", 0),
                "upgrade_1": s.get("upgrade_1", 0),
                "depleted": s.get("depleted", 0),
                "N": s["N"],
            }
        )

    return _finish_building_tiers_from_items(items, k)


def main(template_path, output_dir):
    from generateSocialsPost import create_spec_popularity_vs_performance_img # local import so we don't get circular dependency issues
    print("Generating index page...")
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(template_path)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    env.filters["humanize"] = humanize_number
    env.filters["duration"] = format_duration
    env.filters["format_ts"] = format_utc_timestamp
    env.filters["upgrade_info"] = upgrade_info
    spec_lookup = load_json(os.path.join(LOOKUP_DIR, "specs.json"))
    class_lookup = load_json(os.path.join(LOOKUP_DIR, "classes.json"))
    dungeon_lookup = load_json(os.path.join(LOOKUP_DIR, "dungeons.json"))
    group_buffs = load_json(os.path.join(LOOKUP_DIR, "groupbuffs.json"))
    notifications = load_json(os.path.join(LOOKUP_DIR, "notifications.json"))
    season_info = load_json(os.path.join(LOOKUP_DIR, "seasonInfo.json"))
    buff_lookup = {b.get("id"): b for b in group_buffs}

    spec_nav = generateSpecNav(spec_lookup, class_lookup)
    template = env.get_template(os.path.basename(template_path))

    token = get_access_token(CLIENT_ID, CLIENT_SECRET)
    current_season = get_current_season_id(token)
    print(f"Fetching database data {current_season}...")
    with closing(databaseConnector.get_connection()) as conn:
        cursor = conn.cursor()
        dungeon_data = databaseConnector.fetch_runs_per_dungeon_per_level(
            conn, cursor, current_season
        )
        spec_data = databaseConnector.fetch_spec_upgrades(conn, cursor, current_season)
        groupbuffs_stats = databaseConnector.fetch_groupbuffs_stats(
            conn, cursor, group_buffs, current_season, 12, 14
        )
    print(groupbuffs_stats)
    print("Building tiers...")
    dungeon_tiers = build_ckmeans_tiers(
        dungeon_lookup, dungeon_data, weight_base=1.6, k=6
    )
    spec_tiers = build_spec_tiers(
        spec_lookup, class_lookup, spec_data, weight_base=1.6, k=6
    )
    buff_tiers = build_buff_tiers(buff_lookup, groupbuffs_stats)

    print("Rendering template...")
    output_html = template.render(
        generated_at=datetime.now(timezone.utc).timestamp(),
        spec_nav=spec_nav,
        dungeon_lookup=dungeon_lookup,
        specs=spec_lookup,
        class_lookup=class_lookup,
        active_page="home",
        notifications=notifications,
        breadcrumbs=[
            {"title": "Home", "href": "/"},
        ],
        dungeon_tiers=dungeon_tiers,
        dungeon_scores_available=bool(dungeon_data),
        spec_tiers=spec_tiers,
        spec_scores_available=bool(spec_data),
        season=current_season,
        role_lookup=ROLE_FOLDERS,
        buff_tiers=buff_tiers,
        buff_lookup=buff_lookup,
        buff_scores_available=bool(groupbuffs_stats),
        season_info=season_info,
    )

    # Write output
    out_path = os.path.join(
        output_dir,
        "index.html",
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output_html)
    print(f"Generated {out_path}")
    print("Generating spec popularity vs performance image...")
    preview_path = os.path.join("assets", "img", "previews", "spec_popularity_vs_performance.png")
    os.makedirs(os.path.dirname(preview_path), exist_ok=True)
    create_spec_popularity_vs_performance_img(preview_path, current_season)
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ndex page")
    parser.add_argument(
        "--output_dir", required=True, help="Directory to write generated HTML pages"
    )
    parser.add_argument("--template", required=True, help="Path to HTML template file")
    args = parser.parse_args()
    main(args.template, args.output_dir)
