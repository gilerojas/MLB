"""
Deprecated — use morning_intel/morning_intel.py instead.
"""
import sys


def main():
    print(
        "morning_digest is retired. Run: python morning_intel/morning_intel.py",
        file=sys.stderr,
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
