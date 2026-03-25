"""Regex fallback patterns for season/episode extraction."""

import re

# Each pattern returns (season, episode, confidence) via named groups or positional groups.
# Patterns are tried in order; first match wins.

PATTERNS: list[tuple[re.Pattern, float]] = [
    # SxxExx standard (most reliable)
    (re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})"), 0.3),
    # NxNN format (22x01)
    (re.compile(r"(\d{1,2})x(\d{2,3})"), 0.3),
    # Season word form: Season 22 Episode 1
    (re.compile(r"[Ss]eason\s*(\d{1,2})\s*[Ee]pisode\s*(\d{1,3})"), 0.3),
    # Dense SSEEE encoding (22011 = S22E011) — must be exactly 5 digits
    (re.compile(r"\b(\d{2})(\d{3})\b"), 0.2),
    # Anime episode only: EP01 or E01 (no season, assume S01)
    (re.compile(r"\b[Ee][Pp]?\.?(\d{2,3})\b"), 0.25),
    # Bare number for anime (- 01 -)
    (re.compile(r"\s-\s(\d{2,3})\s"), 0.2),
]


def regex_parse(text: str) -> tuple[int | None, int | None, float]:
    """Try regex patterns against text. Returns (season, episode, confidence).

    Returns (None, None, 0.0) if no pattern matches.
    """
    for pattern, confidence in PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                season = int(groups[0])
                episode = int(groups[1])
                return season, episode, confidence
            elif len(groups) == 1:
                # Anime-style: episode only, assume season 1
                episode = int(groups[0])
                return 1, episode, confidence

    return None, None, 0.0
