"""Multi-season detection and splitting."""

import re
import logging

logger = logging.getLogger(__name__)

# Patterns that indicate a multi-season pack
MULTISEASON_NAME_PATTERNS = [
    re.compile(r"[Ss](\d{1,2})\s*-\s*[Ss](\d{1,2})"),          # S01-S05
    re.compile(r"[Ss]eason[s]?\s*(\d{1,2})\s*-\s*(\d{1,2})"),   # Seasons 1-3, Season 1-3
    re.compile(r"[Cc]omplete\s*[Ss]eries", re.IGNORECASE),       # Complete Series
    re.compile(r"[Cc]omplete\s*[Ss]easons?", re.IGNORECASE),    # Complete Season(s)
    re.compile(r"[Ss](\d{1,2})[Ee]\d+-[Ss](\d{1,2})[Ee]\d+"),   # S01E01-S03E12
]

# Pattern to extract season number from a file path or name
SEASON_FROM_PATH = re.compile(r"[Ss]eason\s*(\d{1,2})", re.IGNORECASE)
SEASON_FROM_SXXEXX = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})")


def detect_multiseason(name: str, file_list: list[str]) -> bool:
    """Detect whether a torrent is a multi-season pack.

    Triggers on:
    - Torrent name contains S01-S05, Seasons 1-3, Complete Series, etc.
    - File list contains files from more than one season number.
    - Folder structure contains multiple Season N directories.
    """
    # Check torrent name
    for pattern in MULTISEASON_NAME_PATTERNS:
        if pattern.search(name):
            return True

    # Check file list for multiple seasons
    seasons_found = set()
    for filepath in file_list:
        # Check folder-based: /Season 1/...
        m = SEASON_FROM_PATH.search(filepath)
        if m:
            seasons_found.add(int(m.group(1)))

        # Check filename-based: S01E01
        m = SEASON_FROM_SXXEXX.search(filepath)
        if m:
            seasons_found.add(int(m.group(1)))

    if len(seasons_found) > 1:
        return True

    return False


def split_into_seasons(file_list: list[str]) -> dict[int, list[str]]:
    """Group files by detected season number.

    Handles both folder-based (/Season 1/...) and filename-based (S01E01) grouping.
    Returns {season_number: [file_paths]}.
    Files that can't be assigned to a season go into season 0.
    """
    result: dict[int, list[str]] = {}

    for filepath in file_list:
        season = _detect_season_for_file(filepath)
        if season not in result:
            result[season] = []
        result[season].append(filepath)

    return result


def _detect_season_for_file(filepath: str) -> int:
    """Detect the season number for a single file path."""
    # Try folder-based first: /Season 1/...
    m = SEASON_FROM_PATH.search(filepath)
    if m:
        return int(m.group(1))

    # Try filename-based: S01E01
    m = SEASON_FROM_SXXEXX.search(filepath)
    if m:
        return int(m.group(1))

    # Unknown season
    return 0
