"""Primary parser using guessit with regex fallback."""

import logging
from guessit import guessit

from debridbridge.parser.models import ParseResult
from debridbridge.parser.normaliser import normalise
from debridbridge.parser.patterns import regex_parse
from debridbridge.parser.multiseason import detect_multiseason, split_into_seasons

logger = logging.getLogger(__name__)


def parse(raw_name: str, file_list: list[str] | None = None) -> ParseResult:
    """Parse a torrent name into structured metadata.

    Args:
        raw_name: Original torrent name.
        file_list: List of file paths inside the torrent (for multi-season detection).

    Returns:
        ParseResult with title, season, episodes, confidence, etc.
    """
    normalised = normalise(raw_name)
    file_list = file_list or []

    # Primary: guessit
    guess = guessit(normalised)

    title = guess.get("title", "")
    year = guess.get("year")
    season = guess.get("season")
    episode = guess.get("episode")
    media_type = guess.get("type", "episode")

    # Normalise episode to a list
    episodes: list[int] = []
    if isinstance(episode, list):
        episodes = [int(e) for e in episode]
    elif episode is not None:
        episodes = [int(episode)]

    # Normalise season
    if isinstance(season, list):
        season = season[0]  # Take first season for primary result
    if season is not None:
        season = int(season)

    # Determine confidence
    confidence = _score_confidence(guess, season, episodes)

    # If guessit failed, try regex fallback
    if confidence < 0.4 and media_type != "movie":
        regex_season, regex_episode, regex_conf = regex_parse(raw_name)
        if regex_conf > 0:
            if season is None:
                season = regex_season
            if not episodes and regex_episode is not None:
                episodes = [regex_episode]
            # Use regex confidence if it's better and guessit didn't find anything
            if regex_conf > confidence:
                confidence = regex_conf
            logger.debug(
                "Regex fallback for '%s': S%sE%s (conf=%.1f)",
                raw_name, regex_season, regex_episode, regex_conf,
            )

    # Multi-season detection
    is_multi = detect_multiseason(raw_name, file_list)
    season_file_map: dict[int, list[str]] = {}
    if is_multi and file_list:
        season_file_map = split_into_seasons(file_list)
        confidence = max(confidence, 0.5)  # Multi-season packs are inherently lower confidence

    # Movie detection
    if media_type == "movie" or (season is None and not episodes and not is_multi):
        # Could be a movie
        if guess.get("type") == "movie" or (year and not season):
            media_type = "movie"
            confidence = max(confidence, 0.7) if title else confidence

    return ParseResult(
        title=title,
        year=year,
        season=season,
        episodes=episodes,
        is_multiseason=is_multi,
        season_file_map=season_file_map,
        confidence=confidence,
        media_type=media_type,
        raw_name=raw_name,
        normalised_name=normalised,
    )


def _score_confidence(guess: dict, season: int | None, episodes: list[int]) -> float:
    """Score parsing confidence based on guessit results."""
    if season is not None and episodes:
        return 1.0
    if guess.get("type") == "movie" and guess.get("title"):
        return 0.8
    if season is not None and not episodes:
        # Season pack (no specific episode)
        return 0.7
    # Check guessit's own confidence via options/raw_options
    if guess.get("title") and (season is not None or episodes):
        return 0.5  # Partial match — guessit uncertain
    if guess.get("title") and not season and not episodes:
        return 0.3  # Title only, no season/episode
    return 0.1  # Both failed
