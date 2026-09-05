#!/usr/bin/env python3
"""Cadence gate for buildPages: should a scheduled run build today?

The site rebuilds hardest right after a content drop (new gear, talent reworks,
meta shifts) and barely moves late in a patch, so the build cadence tracks how
many days have passed since the most recent content update:

    days since latest content update    cadence
    ------------------------------------ ------------------------------
    < 7    (first week)                  every day
    7..41 (weeks 2-6)                   every 3 days (Mon/Wed/Fri)
    >= 42  (after 6 weeks)               once per week (Wed only)

A "content update" is the season start or any retail X.Y.Z patch (the .5/.7
content patches too, not just season launch). Season start comes from
seasonInfo.json `starts.us`; patch go-lives come from patches.json, snapped to
the reset week exactly like the dashboard's patch annotations
(generateDashboardPage.compute_patch_annotations): a patch's first_seen_ts is a
build-push time that leads go-live by a few days, so it is snapped forward to the
first US reset period starting at or after it.

The Wednesday build is the weekly anchor in every phase and is driven by the
getStaticData workflow_run, never by the schedule cron, so this gate only fires
on `schedule` events. push / workflow_dispatch / workflow_run always build.

Emits `phase`, `days_since` and `should_build=true|false` to $GITHUB_OUTPUT (and
stdout). Fails loudly on missing/malformed inputs rather than defaulting to a
build decision.
"""
import argparse
import json
import os
from datetime import datetime, timezone

LOOKUP_DIR = "data/static"

# Phase thresholds in days since the most recent content update. Calendar-day
# based (within a day of the equivalent reset-week boundaries), tunable here.
DAILY_PHASE_DAYS = 7
THREE_DAY_PHASE_DAYS = 42

MS_PER_DAY = 86_400_000

# Weekdays a `three_day` phase still builds on via the schedule cron. Wednesday
# is intentionally absent: it always builds through the getStaticData
# workflow_run, so the cron excludes it. Monday=0 .. Sunday=6.
THREE_DAY_BUILD_WEEKDAYS = {0, 4}  # Monday, Friday


def _emit(**kv):
    line = " ".join(f"{k}={v}" for k, v in kv.items())
    print(line)
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={v}\n")


def _summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text + "\n")


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_now(value):
    """Accept epoch ms (int-like) or an ISO-8601 string; return epoch ms UTC."""
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _season_start_ms(season_info):
    starts = (season_info or {}).get("starts", {})
    us = starts.get("us")
    if not us:
        raise ValueError("seasonInfo.json is missing starts.us")
    return _parse_now(us)


def _us_period_starts(period_info):
    """Sorted ascending list of US reset-period start timestamps (epoch ms)."""
    us = (period_info or {}).get("us", {})
    starts = [int(p["start_timestamp"]) for p in us.get("periods", [])]
    if not starts:
        raise ValueError("periods.json has no US periods")
    return sorted(starts)


def _snap_to_go_live(first_seen_ts, period_starts):
    """First US reset at or after first_seen_ts, or None if not live yet."""
    return next((s for s in period_starts if s >= first_seen_ts), None)


def content_update_go_lives(now_ms, patches, period_info, season_info):
    """Go-live timestamps (epoch ms) of content updates that are live by now_ms.

    Always includes the season start; adds each patch snapped to its reset week.
    """
    period_starts = _us_period_starts(period_info)
    go_lives = [_season_start_ms(season_info)]
    for patch in patches:
        ts = patch.get("first_seen_ts")
        if ts is None:
            continue
        go_live = _snap_to_go_live(int(ts), period_starts)
        if go_live is not None:
            go_lives.append(go_live)
    return [g for g in go_lives if g <= now_ms]


def compute_phase(days_since):
    if days_since < DAILY_PHASE_DAYS:
        return "daily"
    if days_since < THREE_DAY_PHASE_DAYS:
        return "three_day"
    return "weekly"


def should_build(phase, event_name, weekday):
    """weekday: Monday=0 .. Sunday=6. Only `schedule` events are gated."""
    if event_name != "schedule":
        return True
    if phase == "daily":
        return True
    if phase == "three_day":
        return weekday in THREE_DAY_BUILD_WEEKDAYS
    return False  # weekly: Wednesday-only, handled by the workflow_run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--event-name",
        default=os.environ.get("GITHUB_EVENT_NAME", ""),
        help="GitHub event that triggered the run; only 'schedule' is gated",
    )
    parser.add_argument(
        "--now",
        default=None,
        help="Override current time (epoch ms or ISO-8601); for testing",
    )
    parser.add_argument(
        "--weekday",
        type=int,
        default=None,
        help="Override UTC weekday (0=Mon..6=Sun); for testing",
    )
    parser.add_argument("--patches", default=os.path.join(LOOKUP_DIR, "patches.json"))
    parser.add_argument("--periods", default=os.path.join(LOOKUP_DIR, "periods.json"))
    parser.add_argument(
        "--season-info", default=os.path.join(LOOKUP_DIR, "seasonInfo.json")
    )
    args = parser.parse_args()

    if args.now is not None:
        now_ms = _parse_now(args.now)
    else:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if args.weekday is not None:
        weekday = args.weekday
    else:
        weekday = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).weekday()

    patches = _load_json(args.patches)
    period_info = _load_json(args.periods)
    season_info = _load_json(args.season_info)

    go_lives = content_update_go_lives(now_ms, patches, period_info, season_info)
    if not go_lives:
        # No content update is live yet (now precedes the season start): the
        # pre-season gap. seasonHasData already gates this to a clean green skip,
        # so do not fail the job here; just report a non-building decision.
        _emit(phase="pre_season", days_since="-1", should_build="false")
        _summary(
            "Cadence gate: pre-season (no content update live yet); not building. "
            "The season-has-data gate governs the pre-season skip."
        )
        return
    most_recent = max(go_lives)
    days_since = (now_ms - most_recent) / MS_PER_DAY
    phase = compute_phase(days_since)
    build = should_build(phase, args.event_name, weekday)

    most_recent_iso = datetime.fromtimestamp(
        most_recent / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    weekday_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][weekday]

    _emit(
        phase=phase,
        days_since=f"{days_since:.1f}",
        should_build="true" if build else "false",
    )
    verdict = "building" if build else "skipping"
    _summary(
        f"Cadence gate: **{phase}** phase, {days_since:.1f} days since the last "
        f"content update ({most_recent_iso}). Event `{args.event_name or 'n/a'}` "
        f"on {weekday_name} -> {verdict}."
    )


if __name__ == "__main__":
    main()
