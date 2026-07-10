"""Static, LLM-free blog + title generation.

The blog copy shown on the site is built from fixed templates with the run
data dropped into placeholders. This keeps it accurate and consistent; only
the social post (llm.py) is written by a model.
"""

import hashlib
import random

_RUN_KIND = {
    "highest_run": "highest",
    "longest_run": "longest",
    "shortest_run": "fastest",
}


def _join_paragraphs(parts):
    """Join non-empty paragraphs with a blank line, matching the blog splitter."""
    return "\n\n".join(p.strip() for p in parts if p and p.strip())


def _blog_rng(*parts):
    """Deterministic RNG seeded from the post's facts.

    Picking phrasing variants through this keeps the blog copy varied across
    posts while staying stable for a given post: the same facts always render
    the same text, so rebuilding socials.json never churns existing entries.
    """
    key = "|".join("" if p is None else str(p) for p in parts)
    seed = int(hashlib.md5(key.encode("utf-8")).hexdigest()[:12], 16)
    return random.Random(seed)


def build_static_title(post_type, data):
    """Deterministic blog headline for a post, derived from its facts."""
    if post_type in _RUN_KIND:
        kind = _RUN_KIND[post_type].capitalize()
        return f"{kind} Run: +{data.get('level')} {data.get('dungeon')}"
    if post_type == "spec_overview":
        return f"{data.get('spec', '').strip()} Mythic+ Overview"
    if post_type == "dungeon_overview":
        return f"{data.get('dungeon', '').strip()} Dungeon Overview"
    if post_type == "comp_overview":
        return "Global Top Comps"
    if post_type == "dungeon_tierlist":
        return "Dungeon Tier List"
    if post_type == "spec_popularity_tierlist":
        return "Spec Popularity Tier List"
    if post_type == "spec_distribution_by_level":
        return "Spec Distribution Across Key Levels"
    if post_type == "dungeon_popularity_by_level":
        return "Dungeon Popularity Across Key Levels"
    if post_type == "spec_popularity_vs_performance":
        return "Spec Popularity vs Performance"
    return "Mythic+ Data Spotlight"


def build_static_blog(post_type, data):
    """Deterministic blog copy for a post, built from its facts.

    Two short, fact-carrying paragraphs; no generic call-to-action filler (the
    card's "View the data" button already covers that). Phrasing variants are
    chosen through _blog_rng so the copy reads varied across posts but is stable
    for identical facts. Every number comes straight from `data` and is never
    recomputed here.
    """
    if post_type in _RUN_KIND:
        kind = _RUN_KIND[post_type]  # highest / longest / fastest
        level = data.get("level")
        dungeon = data.get("dungeon")
        duration = data.get("duration")
        region = data.get("region")
        where = f" on the {region} region" if region else ""
        run_happened = data.get("run_happened")
        comp = data.get("comp")
        rng = _blog_rng(post_type, level, dungeon, duration, region)

        leads = {
            "highest": [
                f"The highest Mythic+ key MythiStone has tracked this season is a +{level} {dungeon}, timed in {duration}{where}.",
                f"A +{level} {dungeon} cleared in {duration}{where} stands as the highest key on record this season.",
            ],
            "longest": [
                f"The longest Mythic+ run tracked this season is a +{level} {dungeon} that ground on for {duration}{where}.",
                f"At {duration}{where}, this +{level} {dungeon} is the longest single key MythiStone has recorded this season.",
            ],
            "fastest": [
                f"The fastest Mythic+ clear tracked this season is a +{level} {dungeon}, done in just {duration}{where}.",
                f"A +{level} {dungeon} blitzed in {duration}{where} is the quickest clear on record this season.",
            ],
        }
        p1 = rng.choice(leads.get(kind, leads["highest"]))

        tail = []
        if comp:
            tail.append(rng.choice([
                f"The five who pulled it off: {comp}.",
                f"Credit the group that ran it: {comp}.",
            ]))
        if run_happened:
            tail.append(rng.choice([
                f"It went down {run_happened}, and records like it only stand until the next group pushes higher.",
                f"That was {run_happened}. Every record here keeps moving as new keys get pushed.",
            ]))
        else:
            tail.append("Records like it only stand until the next group pushes higher.")
        return _join_paragraphs([p1, " ".join(tail)])

    if post_type == "spec_overview":
        spec = (data.get("spec") or "").strip()
        runs = data.get("amount_data_source_runs")
        name = data.get("top_hero_tree_name")
        pct = data.get("top_hero_tree_pct")
        runner = data.get("runner_up_hero_tree")
        timed = data.get("timed_pct")
        three = data.get("three_chest_pct")
        stats = data.get("stat_priority")
        highest = data.get("highest_run")
        rng = _blog_rng("spec_overview", spec, runs)

        p1 = rng.choice([
            f"{spec} has {runs} Mythic+ runs tracked this season.",
            f"This season MythiStone has logged {runs} {spec} runs.",
            f"{runs} {spec} keys are in the books this season.",
        ])
        if name and pct:
            if runner:
                p1 += rng.choice([
                    f" The {name} hero tree leads at {pct} of builds, with {runner} trailing.",
                ])
            else:
                p1 += rng.choice([
                    f" The {name} hero tree is the runaway pick at {pct} of builds.",
                ])

        facts = []
        if timed and three:
            facts.append(rng.choice([
                f"Groups time keys with the spec {timed} of the time and three-chest {three} of them.",
                f"{timed} of tracked runs beat the timer, and {three} earn all three chests.",
            ]))
        elif timed:
            facts.append(f"{timed} of tracked runs beat the timer.")
        if stats:
            facts.append(rng.choice([
                f"The stat priority skews toward {stats}.",
                f"Most builds prioritise {stats}.",
            ]))
        if highest:
            facts.append(rng.choice([
                f"The best key so far: {highest}.",
                f"Its top run this season is {highest}.",
            ]))
        return _join_paragraphs([p1, " ".join(facts)])

    if post_type == "dungeon_overview":
        dungeon = (data.get("dungeon") or "").strip()
        runs = data.get("amount_data_source_runs")
        route = data.get("top_route")
        comp = data.get("top_comp")
        rng = _blog_rng("dungeon_overview", dungeon, runs)

        p1 = rng.choice([
            f"{dungeon} has {runs} Mythic+ runs tracked this season.",
            f"This overview of {dungeon} draws on {runs} tracked Mythic+ runs.",
        ])
        facts = []
        if comp:
            facts.append(rng.choice([
                f"The most common group through it is {comp}.",
                f"Groups most often bring {comp}.",
            ]))
        if route and route != "Unknown":
            facts.append(rng.choice([
                f"The most-used route right now is {route}.",
                f"Most players follow the {route} route.",
            ]))
        if not facts:
            facts.append("The dungeon page breaks down the comps and routes groups rely on to time it.")
        return _join_paragraphs([p1, " ".join(facts)])

    if post_type == "comp_overview":
        runs = data.get("amount_data_source_runs")
        top = data.get("top_comp")
        runner = data.get("runner_up_comp")
        flex = data.get("most_flexible_spec")
        rng = _blog_rng("comp_overview", runs, top)

        p1 = rng.choice([
            f"Across {runs} tracked Mythic+ runs, the most popular group composition is {top}.",
            f"{top} is the most-run Mythic+ composition across {runs} tracked runs.",
        ])
        facts = []
        if runner:
            facts.append(rng.choice([
                f"The runner-up is {runner}.",
                f"{runner} sits just behind it.",
            ]))
        if flex:
            facts.append(rng.choice([
                f"{flex} is the most flexible spec, fitting into more comps than any other.",
                f"No spec slots into more comps than {flex}.",
            ]))
        if not facts:
            facts.append("See every top comp and the most flexible specs on the comps page.")
        return _join_paragraphs([p1, " ".join(facts)])

    if post_type == "dungeon_tierlist":
        runs = data.get("total_runs")
        best = data.get("best_dungeon")
        worst = data.get("worst_dungeon")
        sb = data.get("second_best_dungeon")
        sw = data.get("second_worst_dungeon")
        rng = _blog_rng("dungeon_tierlist", runs, best, worst)

        p1 = rng.choice([
            f"Based on {runs} tracked Mythic+ runs, {best} tops this season's dungeon tier list while {worst} sits at the bottom.",
            f"{best} leads the dungeon tier list this season and {worst} anchors the bottom, across {runs} tracked runs.",
        ])
        facts = []
        if sb and sw:
            facts.append(f"{sb} follows near the top, and {sw} is not far off the bottom.")
        elif sb:
            facts.append(f"{sb} follows just behind at the top.")
        elif sw:
            facts.append(f"{sw} sits just above the bottom.")
        facts.append("Tiers reflect how cleanly groups are timing each dungeon at higher keys.")
        return _join_paragraphs([p1, " ".join(facts)])

    if post_type == "spec_popularity_tierlist":
        most = data.get("most_popular_spec") or {}
        least = data.get("least_popular_spec") or {}
        runs = data.get("total_runs")
        rng = _blog_rng("spec_popularity_tierlist", runs, most.get("name"))

        p1 = rng.choice([
            f"Across {runs} tracked Mythic+ runs this season, {most.get('name')} is the most-played spec with {most.get('runs')} runs.",
            f"{most.get('name')} tops the popularity tier list this season at {most.get('runs')} runs, out of {runs} tracked overall.",
        ])
        p2 = rng.choice([
            f"At the other end, {least.get('name')} is the least represented with {least.get('runs')} runs. Popularity is not the same as power, but it shows what the community is actually bringing.",
            f"{least.get('name')} brings up the rear at {least.get('runs')} runs. What is popular is not always what is strongest, but it does reflect what players pick.",
        ])
        return _join_paragraphs([p1, p2])

    if post_type == "spec_distribution_by_level":
        specs = data.get("highest_specs") or []
        lvl = data.get("highest_keylevel")
        rng = _blog_rng("spec_distribution_by_level", lvl, ",".join(specs))

        if specs and lvl is not None:
            p1 = rng.choice([
                f"At the very top of the ladder, level {lvl}, the most common specs are {', '.join(specs)}.",
                f"Once keys reach level {lvl}, the field narrows to a handful of specs: {', '.join(specs)}.",
            ])
        else:
            p1 = "This breakdown shows how spec representation shifts as Mythic+ keys climb."
        p2 = rng.choice([
            "Lower keys stay far more varied; the highest levels concentrate around a few specs.",
            "The spread is wide at low keys and tightens sharply toward the top.",
        ])
        return _join_paragraphs([p1, p2])

    if post_type == "dungeon_popularity_by_level":
        levels = data.get("levels_covered")
        top = data.get("top_dungeon")
        bottom = data.get("bottom_dungeon")
        rng = _blog_rng("dungeon_popularity_by_level", levels, top, bottom)

        p1 = rng.choice([
            f"Across {levels} key levels, {top} is the most-run dungeon while {bottom} sees the fewest completions.",
            f"{top} draws the most runs across {levels} key levels; {bottom} draws the fewest.",
        ])
        p2 = rng.choice([
            "Dungeon popularity tracks route length, difficulty and how punishing the bosses get at high keys.",
            "Route length, boss difficulty and trash density all steer where groups spend their keys.",
        ])
        return _join_paragraphs([p1, p2])

    if post_type == "spec_popularity_vs_performance":
        over = data.get("most_overperforming_spec")
        under = data.get("most_underperforming_spec")
        rng = _blog_rng("spec_popularity_vs_performance", over, under)

        p1 = rng.choice([
            f"Plotting popularity against performance, {over} is the biggest overperformer, punching above its play rate.",
            f"{over} stands out as the biggest overperformer, doing more than its popularity would suggest.",
        ])
        p2 = rng.choice([
            f"On the flip side, {under} is played more than its results justify.",
            f"{under}, meanwhile, is more popular than its performance would predict.",
        ])
        return _join_paragraphs([p1, p2])

    # unknown type: no static copy, blog card just shows the title + image
    return ""
