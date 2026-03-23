#!/usr/bin/env python3
"""
Launcher: run the WBC pitcher card script from the WBC project.
Use this when you are in the MLB directory; forwards all arguments.

  python scripts/wbc_pitcher_card.py --game-pk 788106 --pitcher-id 622663
"""
import os
import subprocess
import sys
from pathlib import Path

_MLB_ROOT = Path(__file__).resolve().parent.parent
_WBC_ROOT = _MLB_ROOT.parent / "WBC"
_WBC_SCRIPT = _WBC_ROOT / "scripts" / "wbc_pitcher_card.py"

if not _WBC_SCRIPT.is_file():
    print(f"ERROR: WBC script not found: {_WBC_SCRIPT}", file=sys.stderr)
    sys.exit(1)

os.chdir(_WBC_ROOT)
sys.exit(subprocess.run([sys.executable, str(_WBC_SCRIPT)] + sys.argv[1:]).returncode)
