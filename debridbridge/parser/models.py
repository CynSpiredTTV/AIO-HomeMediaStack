"""Parse result dataclass."""

from dataclasses import dataclass, field


@dataclass
class ParseResult:
    title: str
    year: int | None
    season: int | None
    episodes: list[int] = field(default_factory=list)  # empty = full season
    is_multiseason: bool = False
    season_file_map: dict[int, list[str]] = field(default_factory=dict)  # season -> file paths
    confidence: float = 0.0
    media_type: str = "episode"  # 'movie' | 'episode'
    raw_name: str = ""
    normalised_name: str = ""
