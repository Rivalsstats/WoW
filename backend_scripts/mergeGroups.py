#!/usr/bin/env python3
import os
import json
import argparse

from combineRealms import combine_group_data


def main():
    p = argparse.ArgumentParser(
        description="Merge all per-group caches into final aggregates"
    )
    p.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing unpacked group combined data",
    )
    p.add_argument(
        "--output-dir",
        required=True,
        help="Where to write `season_summary.json` and specs/",
    )
    args = p.parse_args()

    # Run merge
    merged = combine_group_data(args.input_dir)

    print(f"Merged data: {len(merged['specs'])} specs")
    # Ensure output structure
    os.makedirs(args.output_dir, exist_ok=True)

    # Write season_summary.json
    with open(os.path.join(args.output_dir, "season_summary.json"), "w") as f:
        json.dump(merged["season_summary"], f, indent=2)

    # Write each spec/general + per-dungeon JSON
    for (season, spec), spec_data in merged["specs"].items():
        print(
            f"Writing spec {season}/{spec} with {len(spec_data['dungeons'])} dungeons"
        )
        base = os.path.join(args.output_dir, "specs", season, spec)
        os.makedirs(base, exist_ok=True)
        with open(os.path.join(base, "general.json"), "w") as f:
            json.dump(spec_data["general"], f, indent=2)

        for (dungeon, lvl), details in spec_data["dungeons"].items():
            ddir = os.path.join(base, "dungeons", dungeon)
            os.makedirs(ddir, exist_ok=True)
            with open(os.path.join(ddir, f"{lvl}.json"), "w") as f:
                json.dump(details, f, indent=2)

    print(
        f"Wrote {len(merged['specs'])} specs to {args.output_dir}/specs/ and season summary to {args.output_dir}/season_summary.json"
    )


if __name__ == "__main__":
    main()
