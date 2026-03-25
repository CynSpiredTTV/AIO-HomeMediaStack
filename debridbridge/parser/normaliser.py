"""String cleaning and normalisation before parsing."""

import re

# Quality tags to strip
QUALITY_TAGS = re.compile(
    r"\b("
    r"720p|1080p|1080i|2160p|4K|UHD|"
    r"BluRay|Blu-Ray|BDRip|BRRip|"
    r"WEB-DL|WEBRip|WEB|WEBDL|"
    r"HDTV|HDRip|DVDRip|DVD|PDTV|SDTV|"
    r"x264|x265|H\.264|H\.265|HEVC|AVC|"
    r"AAC|DDP5\.1|DD5\.1|DTS|FLAC|AC3|Atmos|TrueHD|"
    r"REMUX|PROPER|REPACK|INTERNAL|"
    r"10bit|HDR|HDR10|HDR10Plus|DV|DoVi|"
    r"AMZN|NF|DSNP|HMAX|ATVP|PCOK|PMTP"
    r")\b",
    re.IGNORECASE,
)

# Release group suffix: -GROUP at end of string (must start with dash, not space)
# Only match when preceded by a dash to avoid stripping S01E01 etc.
RELEASE_GROUP = re.compile(r"-([A-Za-z][A-Za-z0-9]+)$")

# Illegal filename characters
ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*]')


def normalise(name: str) -> str:
    """Clean a torrent name for parsing.

    Steps:
    1. Normalise S22:E01 → S22E01
    2. Replace dots and underscores with spaces
    3. Strip quality tags
    4. Strip release group suffix
    5. Trim whitespace
    """
    result = name

    # Normalise colon-separated season/episode: S22:E01 → S22E01
    result = re.sub(r"[Ss](\d{1,2}):?[Ee](\d{1,3})", r"S\1E\2", result)

    # Replace dots and underscores with spaces
    result = re.sub(r"[._]", " ", result)

    # Replace remaining colons with " - " (e.g., "Star Trek: Discovery" → "Star Trek - Discovery")
    # but NOT colons that were already handled above (S22:E01 → S22E01)
    result = re.sub(r":", " - ", result)

    # Strip quality tags
    result = QUALITY_TAGS.sub("", result)

    # Strip release group suffix (e.g., "- GROUP" at end)
    result = RELEASE_GROUP.sub("", result)

    # Collapse multiple spaces
    result = re.sub(r"\s+", " ", result).strip()

    return result


def sanitise_filename(name: str) -> str:
    """Remove illegal filesystem characters from a filename."""
    return ILLEGAL_CHARS.sub("", name).strip()
