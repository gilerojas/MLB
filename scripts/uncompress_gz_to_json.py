"""
One-off: decompress raw feed_live .json.gz files to .json and remove .gz.

Run from repo root:
  python scripts/uncompress_gz_to_json.py [--warehouse data/warehouse/mlb] [--dry-run] [--keep-gz]

Use --dry-run to only print what would be decompressed.
Use --keep-gz to leave .gz in place after writing .json (default: remove .gz).
"""
import argparse
import gzip
import shutil
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description="Decompress raw *_feed_live.json.gz to .json")
    p.add_argument("--warehouse", type=Path, default=Path("data/warehouse/mlb"))
    p.add_argument("--dry-run", action="store_true", help="Only print, do not write or delete")
    p.add_argument("--keep-gz", action="store_true", help="Keep .gz files after decompressing")
    args = p.parse_args()

    warehouse = args.warehouse.resolve()
    if not warehouse.is_dir():
        p.error(f"Warehouse not found: {warehouse}")

    to_decompress = list(warehouse.rglob("raw/*_feed_live.json.gz"))
    # Skip if .json already exists for same base (unless we add --force later)
    to_decompress = [
        p for p in to_decompress
        if not (p.parent / p.name.replace(".json.gz", ".json")).exists()
    ]

    if not to_decompress:
        print("No *_feed_live.json.gz files to decompress (or .json already exists).")
        return

    print(f"Found {len(to_decompress)} .json.gz file(s) to decompress.")
    if args.dry_run:
        for path in to_decompress[:10]:
            print(f"  {path.relative_to(warehouse)}")
        if len(to_decompress) > 10:
            print(f"  ... and {len(to_decompress) - 10} more")
        print("Run without --dry-run to decompress (and remove .gz unless --keep-gz).")
        return

    ok = 0
    for gz_path in to_decompress:
        json_path = gz_path.parent / gz_path.name.replace(".json.gz", ".json")
        try:
            with gzip.open(gz_path, "rt", encoding="utf-8") as f_in:
                with open(json_path, "w", encoding="utf-8") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            if not args.keep_gz:
                gz_path.unlink()
            ok += 1
        except Exception as e:
            print(f"  Error {gz_path}: {e}")
    print(f"Decompressed {ok} file(s)." + ("" if args.keep_gz else " Removed .gz."))


if __name__ == "__main__":
    main()
