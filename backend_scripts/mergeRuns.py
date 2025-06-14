import argparse
import csv
import os
from pathlib import Path

def merge_csv(old_dir: Path, new_dir: Path):
    print(f"[DEBUG] Merging CSV files from {new_dir} into {old_dir}")
    for new_csv in new_dir.rglob('runs.csv'):
        rel = new_csv.relative_to(new_dir)
        old_csv = old_dir / rel
        old_csv.parent.mkdir(parents=True, exist_ok=True)

        # read header and existing rows
        if old_csv.exists():
            with old_csv.open(newline='') as f:
                reader = csv.reader(f)
                old_header = next(reader)
                old_rows = list(reader)
        else:
            old_header = None
            old_rows = []

        # read new data
        with new_csv.open(newline='') as f:
            reader = csv.reader(f)
            new_header = next(reader)
            new_rows = list(reader)

        # choose header
        header = old_header or new_header

        # append only rows not in old_rows
        existing = set(tuple(r) for r in old_rows)
        merged = old_rows.copy()
        for row in new_rows:
            trow = tuple(row)
            if trow not in existing:
                merged.append(row)

        # write back
        out_path = old_csv
        with out_path.open('w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(merged)
        print(f"[DEBUG] Merged {len(new_rows)} new rows into {out_path}")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--old-dir', type=Path, required=True)
    p.add_argument('--new-dir', type=Path, required=True)
    args = p.parse_args()
    merge_csv(args.old_dir, args.new_dir)