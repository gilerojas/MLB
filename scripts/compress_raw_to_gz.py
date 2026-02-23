"""
One-off: compress existing raw feed_live JSON files to .json.gz and remove originals.

Run from repo root:
  python scripts/compress_raw_to_gz.py [--warehouse data/warehouse/mlb] [--dry-run]

Use --dry-run to only print what would be compressed.
"""
import argparse
import gzip
import json
import shutil
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Compress raw feed_live.json to .json.gz")
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse/mlb"))
    p.add_argument("--dry-run", action="store_true", help="Only print, do not write or delete")
    args = p.parse_args()

    warehouse = args.warehouse.resolve()
    if not warehouse.is_dir():
        p.error(f"Warehouse not found: {warehouse}")

    to_compress = list(warehouse.rglob("raw/*_feed_live.json"))
    # Skip if .json.gz already exists for same base name
    to_compress = [p for p in to_compress if not (p.parent / (p.stem + ".json.gz")).exists()]

    if not to_compress:
        print("No uncompressed *_feed_live.json files found (or .gz already exists).")
        return

    print(f"Found {len(to_compress)} raw JSON file(s) to compress.")
    if args.dry_run:
        for path in to_compress[:5]:
            print(f"  {path.relative_to(warehouse)}")
        if len(to_compress) > 5:
            print(f"  ... and {len(to_compress) - 5} more")
        print("Run without --dry-run to compress and remove originals.")
        return

    ok = 0
    for path in to_compress:
        gz_path = path.parent / f"{path.stem}.json.gz"
        try:
            with open(path, "rb") as f_in:
                with gzip.open(gz_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            path.unlink()
            ok += 1
        except Exception as e:
            print(f"  Error {path}: {e}")
    print(f"Compressed and removed {ok} file(s).")


if __name__ == "__main__":
    main()
