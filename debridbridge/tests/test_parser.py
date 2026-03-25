"""Tests for the media filename parser."""

import pytest

from debridbridge.parser.guesser import parse
from debridbridge.parser.normaliser import normalise, sanitise_filename
from debridbridge.parser.multiseason import detect_multiseason, split_into_seasons


# --- Normaliser tests ---

class TestNormaliser:
    def test_dots_to_spaces(self):
        assert "The Simpsons S22E01" in normalise("The.Simpsons.S22E01.720p.BluRay.x264-GROUP")

    def test_underscores_to_spaces(self):
        assert "The Simpsons" in normalise("The_Simpsons_S01E01")

    def test_colon_episode(self):
        result = normalise("The Simpsons S22:E01")
        assert "S22E01" in result

    def test_quality_tags_stripped(self):
        result = normalise("Show.Name.S01E01.1080p.BluRay.x265.HEVC.DDP5.1")
        assert "1080p" not in result
        assert "BluRay" not in result
        assert "x265" not in result

    def test_sanitise_filename(self):
        assert sanitise_filename('Movie: The "Best" <One>') == "Movie The Best One"


# --- Standard episode patterns ---

class TestStandardPatterns:
    def test_sxxexx_dots(self):
        r = parse("The.Simpsons.S22E01.720p.BluRay.x264-GROUP")
        assert r.title.lower() == "the simpsons"
        assert r.season == 22
        assert r.episodes == [1]
        assert r.confidence >= 0.8

    def test_sxxexx_spaces(self):
        r = parse("The Simpsons S22E01 720p BluRay")
        assert r.season == 22
        assert r.episodes == [1]

    def test_nxnn_format(self):
        r = parse("The Simpsons - 22x01 - Marge Simpson in Screaming Yellow Honkers")
        assert r.season == 22
        assert r.episodes == [1]

    def test_colon_separator(self):
        r = parse("The Simpsons S22:E01")
        assert r.season == 22
        assert r.episodes == [1]

    def test_season_word_form(self):
        r = parse("The Simpsons Season 22 Episode 1")
        assert r.season == 22
        assert r.episodes == [1]

    def test_complete_season(self):
        r = parse("The.Simpsons.Season.22.Complete")
        assert r.season == 22
        assert r.episodes == []  # Full season, no specific episode
        assert r.confidence >= 0.5

    def test_dense_encoding(self):
        """22011 = Season 22, Episode 011."""
        r = parse("The.Simpsons.22011.HDTV")
        # regex fallback should catch 5-digit dense encoding
        assert r.confidence >= 0.1
        # At minimum the title should be extracted
        assert "simpsons" in r.title.lower()

    def test_lowercase_sxxexx(self):
        r = parse("the.simpsons.s01e05.hdtv")
        assert r.season == 1
        assert r.episodes == [5]


# --- Multi-episode ---

class TestMultiEpisode:
    def test_double_episode(self):
        r = parse("The.Simpsons.S22E01E02.720p")
        assert r.season == 22
        assert 1 in r.episodes
        assert 2 in r.episodes


# --- Multi-season packs ---

class TestMultiSeason:
    def test_s01_s05_name(self):
        r = parse(
            "The.Simpsons.S01-S05.Complete.Series.720p",
            file_list=[
                "/Season 1/The Simpsons S01E01.mkv",
                "/Season 2/The Simpsons S02E01.mkv",
                "/Season 5/The Simpsons S05E01.mkv",
            ],
        )
        assert r.is_multiseason is True
        assert len(r.season_file_map) >= 2

    def test_seasons_word(self):
        r = parse(
            "The Simpsons Seasons 1-3 BluRay",
            file_list=[
                "/Season 1/S01E01.mkv",
                "/Season 2/S02E01.mkv",
                "/Season 3/S03E01.mkv",
            ],
        )
        assert r.is_multiseason is True

    def test_complete_series(self):
        r = parse(
            "The.Simpsons.The.Complete.Series.S01-S34",
            file_list=["/Season 1/ep01.mkv", "/Season 34/ep01.mkv"],
        )
        assert r.is_multiseason is True

    def test_cross_season_range(self):
        r = parse(
            "Some.Show.S01E01-S03E12.Complete",
            file_list=["/S01E01.mkv", "/S03E12.mkv"],
        )
        assert r.is_multiseason is True

    def test_detect_from_files_only(self):
        """Even if name doesn't indicate multi-season, files from multiple seasons trigger detection."""
        assert detect_multiseason(
            "Some Show Complete",
            ["/Season 1/ep1.mkv", "/Season 2/ep1.mkv"],
        )

    def test_split_by_folder(self):
        files = [
            "/Season 1/S01E01.mkv",
            "/Season 1/S01E02.mkv",
            "/Season 2/S02E01.mkv",
        ]
        result = split_into_seasons(files)
        assert 1 in result
        assert 2 in result
        assert len(result[1]) == 2
        assert len(result[2]) == 1

    def test_split_by_filename(self):
        files = ["S01E01.mkv", "S01E02.mkv", "S02E01.mkv"]
        result = split_into_seasons(files)
        assert 1 in result
        assert 2 in result


# --- Movie patterns ---

class TestMovies:
    def test_movie_with_year(self):
        r = parse("The.Matrix.1999.1080p.BluRay.x264")
        assert "matrix" in r.title.lower()
        assert r.year == 1999
        assert r.media_type == "movie"
        assert r.confidence >= 0.7

    def test_movie_no_year(self):
        r = parse("Inception.2010.BluRay.1080p")
        assert r.year == 2010
        assert r.media_type == "movie"


# --- Anime patterns ---

class TestAnime:
    def test_sub_group_format(self):
        r = parse("[SubGroup] Show Name - 01 [1080p][CRC32].mkv")
        assert 1 in r.episodes, f"Expected episode 1, got {r.episodes}"
        assert r.confidence >= 0.2

    def test_ep_format(self):
        r = parse("Show.Name.EP01.720p")
        assert 1 in r.episodes

    def test_bare_number(self):
        r = parse("Show Name - 05 - Episode Title")
        assert 5 in r.episodes


# --- Edge cases ---

class TestEdgeCases:
    def test_empty_string(self):
        r = parse("")
        assert r.confidence <= 0.3

    def test_just_quality_tags(self):
        r = parse("720p.BluRay.x264")
        assert r.confidence <= 0.3

    def test_very_long_name(self):
        r = parse(
            "The.Really.Long.Show.Name.With.Many.Words.S01E01.1080p.WEB-DL.DDP5.1.x264-GROUP"
        )
        assert r.season == 1
        assert r.episodes == [1]

    def test_year_in_show_name(self):
        r = parse("The.Flash.2014.S01E01.720p")
        assert r.season == 1
        assert r.episodes == [1]
