"""Morning intel package.

Without this marker ``morning_intel/morning_intel.py`` shadows the directory as a
plain module whenever a script inside it is run directly, which turns the sibling
imports below into a circular import.
"""
