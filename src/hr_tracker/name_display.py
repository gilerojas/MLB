"""Compact display names for HR tracker text and images."""

_ROMAN_NUMERAL_SUFFIXES = frozenset(
    {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}
)


def _is_generational_suffix(tok: str) -> bool:
    return tok.upper().rstrip(".") in ("JR", "SR")


def _is_roman_suffix(tok: str) -> bool:
    return tok.upper() in _ROMAN_NUMERAL_SUFFIXES


def last_name_compact(full_name: str) -> str:
    """Last word of full name for short display; skips Jr./Sr. and Roman numerals at the end."""
    if not full_name:
        return "?"
    parts = full_name.strip().split()
    if len(parts) == 1:
        return full_name
    while len(parts) > 1:
        last = parts[-1]
        if _is_generational_suffix(last) or _is_roman_suffix(last):
            parts = parts[:-1]
        else:
            break
    return parts[-1] if len(parts) > 1 else parts[0]


def last_name_with_generational_suffix(full_name: str) -> str:
    """
    Family name plus trailing Jr./Sr./roman numerals — avoids bare \"Jr.\" when the feed
    uses \"Robert Jr.\" or \"Vladimir Guerrero Jr.\".
    """
    if not full_name:
        return "?"
    parts = full_name.strip().split()
    if not parts:
        return "?"
    suffixes: list[str] = []
    while parts and (_is_generational_suffix(parts[-1]) or _is_roman_suffix(parts[-1])):
        suffixes.insert(0, parts.pop())
    if not parts:
        return "?"
    family = parts[-1]
    if suffixes:
        return f"{family} {' '.join(suffixes)}"
    return family
