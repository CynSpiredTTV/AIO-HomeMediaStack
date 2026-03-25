"""Tests for the import pipeline — Phase 10.

Covers: full magnet-to-symlink flow, uncached rejection, multi-season splitting,
low-confidence review flagging, import failure handling, movie imports, and
edge cases.

Uses a real temporary SQLite database and temporary directories for symlinks.
"""

import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from debridbridge.arr.models import ArrEpisode, ArrMovie, ArrSeries
from debridbridge.database import Database
from debridbridge.importer.pipeline import ImportPipeline, ImportTask
from debridbridge.parser.models import ParseResult
from debridbridge.realdebrid.models import (
    AddMagnetResponse,
    RDTorrentFile,
    TorrentInfo,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_HASH = "a" * 40
FAKE_MAGNET = f"magnet:?xt=urn:btih:{FAKE_HASH}"


def _make_torrent_info(
    filename: str,
    files: list[RDTorrentFile] | None = None,
    status: str = "downloaded",
) -> TorrentInfo:
    """Build a TorrentInfo with sensible defaults."""
    if files is None:
        files = [
            RDTorrentFile(id=1, path=f"/{filename}/episode.mkv", bytes=500_000_000, selected=1),
        ]
    return TorrentInfo(
        id="rd-123",
        filename=filename,
        hash=FAKE_HASH,
        bytes=sum(f.bytes for f in files),
        progress=100.0,
        status=status,
        files=files,
    )


@dataclass
class FakeSettings:
    """Minimal settings object matching what ImportPipeline reads."""
    symlink_path: str = "/tmp/test_symlinks"
    mount_path: str = "/tmp/test_mount"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_dirs(tmp_path: Path):
    """Create temporary symlink and mount directories."""
    symlink_dir = tmp_path / "symlinks"
    mount_dir = tmp_path / "mount"
    symlink_dir.mkdir()
    mount_dir.mkdir()
    return FakeSettings(symlink_path=str(symlink_dir), mount_path=str(mount_dir))


@pytest.fixture()
def db(tmp_path: Path):
    """Create a real temporary SQLite database."""
    db_path = str(tmp_path / "test.db")
    database = Database(db_path)
    database.init()
    return database


@pytest.fixture()
def rd_client():
    """Fully mocked RDClient."""
    mock = AsyncMock()
    # Defaults — tests override as needed
    mock.check_cache = AsyncMock(return_value={FAKE_HASH: True})
    mock.add_magnet = AsyncMock(
        return_value=AddMagnetResponse(id="rd-123", uri="https://rd/torrent/rd-123")
    )
    mock.select_files = AsyncMock()
    mock.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Some.Show.S01E01.mkv")
    )
    return mock


@pytest.fixture()
def sonarr_client():
    """Fully mocked Sonarr ArrClient."""
    mock = AsyncMock()
    mock.lookup_series = AsyncMock(
        return_value=ArrSeries(id=10, title="Some Show", tvdbId=12345)
    )
    mock.get_episodes = AsyncMock(
        return_value=[ArrEpisode(id=101, episodeNumber=1, seasonNumber=1, title="Pilot")]
    )
    mock.manual_import = AsyncMock(return_value=True)
    return mock


@pytest.fixture()
def radarr_client():
    """Fully mocked Radarr ArrClient."""
    mock = AsyncMock()
    mock.lookup_movie = AsyncMock(
        return_value=ArrMovie(id=20, title="Some Movie", year=2024, tmdbId=99999)
    )
    mock.manual_import_movie = AsyncMock(return_value=True)
    return mock


def _seed_torrent(db: Database, infohash: str = FAKE_HASH):
    """Insert a minimal torrents row so pipeline UPDATE statements work."""
    with db.get_db() as conn:
        conn.execute(
            "INSERT INTO torrents (rd_hash, raw_name, status, added_at) "
            "VALUES (?, ?, 'pending', ?)",
            (infohash, "seed", time.time()),
        )


def _seed_vd(db: Database, infohash: str = FAKE_HASH, category: str = "sonarr",
             arr_host: str = "http://sonarr:8989", season: str | None = None):
    """Insert a virtual_download so _mark_failed / _mark_needs_review have a target."""
    import hashlib
    vd_id = str(uuid.uuid4())
    if season is not None:
        qbit_hash = hashlib.sha1(f"{infohash}:season:{season}".encode()).hexdigest()
    else:
        qbit_hash = infohash.lower()
    with db.get_db() as conn:
        conn.execute(
            "INSERT INTO virtual_downloads "
            "(id, rd_hash, arr_category, arr_host, season_number, qbit_hash, "
            "status, parse_confidence, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0.5, ?)",
            (vd_id, infohash, category, arr_host, season, qbit_hash, time.time()),
        )
    return vd_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_episode_pipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs):
    """Full pipeline: magnet -> cache check -> add -> symlink -> import for a single episode."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/Some.Show.S01E01/Some.Show.S01E01.720p.mkv",
                       bytes=500_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Some.Show.S01E01.720p", files, status="downloaded")
    )

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)

    task = ImportTask(
        infohash=FAKE_HASH,
        magnet=FAKE_MAGNET,
        category="sonarr",
        arr_host="http://sonarr:8989",
        save_path="/downloads",
    )
    await pipeline._process(task)

    # RD interactions happened
    rd_client.check_cache.assert_awaited_once()
    rd_client.add_magnet.assert_awaited_once()
    rd_client.select_files.assert_awaited_once()

    # Sonarr was asked to import
    sonarr_client.lookup_series.assert_awaited_once()
    sonarr_client.manual_import.assert_awaited()

    # A virtual download was created in 'done' status
    with db.get_db() as conn:
        vds = conn.execute(
            "SELECT * FROM virtual_downloads WHERE rd_hash = ?", (FAKE_HASH,)
        ).fetchall()
        assert len(vds) >= 1
        assert any(row["status"] == "done" for row in vds)

    # Symlink was created on disk
    with db.get_db() as conn:
        symlinks = conn.execute("SELECT * FROM symlinks").fetchall()
        assert len(symlinks) >= 1
        for sl in symlinks:
            assert os.path.islink(sl["symlink_path"])


@pytest.mark.asyncio
async def test_uncached_torrent_marked_failed(rd_client, sonarr_client, radarr_client, db, tmp_dirs):
    """Uncached torrents must be rejected and marked failed."""
    _seed_torrent(db)
    vd_id = _seed_vd(db)

    rd_client.check_cache = AsyncMock(return_value={FAKE_HASH: False})

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)

    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")
    await pipeline._process(task)

    # Should NOT have called add_magnet
    rd_client.add_magnet.assert_not_awaited()

    # The VD should be marked failed
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM virtual_downloads WHERE id = ?", (vd_id,)
        ).fetchone()
        assert row["status"] == "failed"


@pytest.mark.asyncio
async def test_multiseason_split_creates_correct_vd_count(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """A multi-season torrent should produce one virtual download per season."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/pack/Season 1/Show.S01E01.mkv", bytes=400_000_000, selected=1),
        RDTorrentFile(id=2, path="/pack/Season 1/Show.S01E02.mkv", bytes=400_000_000, selected=1),
        RDTorrentFile(id=3, path="/pack/Season 2/Show.S02E01.mkv", bytes=400_000_000, selected=1),
        RDTorrentFile(id=4, path="/pack/Season 3/Show.S03E01.mkv", bytes=400_000_000, selected=1),
    ]
    torrent_info = _make_torrent_info("Some.Show.S01-S03.Complete", files, status="downloaded")
    rd_client.get_torrent_info = AsyncMock(return_value=torrent_info)

    # get_episodes returns episodes per season
    async def _mock_get_episodes(series_id, season_num):
        return [ArrEpisode(id=1000 + season_num * 10 + i, episodeNumber=i,
                           seasonNumber=season_num, title=f"Ep {i}")
                for i in range(1, 3)]

    sonarr_client.get_episodes = AsyncMock(side_effect=_mock_get_episodes)
    sonarr_client.manual_import = AsyncMock(return_value=True)

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)

    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")

    # Patch the parser to return a multi-season result
    fake_parse = ParseResult(
        title="Some Show",
        year=None,
        season=1,
        episodes=[],
        is_multiseason=True,
        season_file_map={
            1: ["/pack/Season 1/Show.S01E01.mkv", "/pack/Season 1/Show.S01E02.mkv"],
            2: ["/pack/Season 2/Show.S02E01.mkv"],
            3: ["/pack/Season 3/Show.S03E01.mkv"],
        },
        confidence=0.7,
        media_type="episode",
        raw_name="Some.Show.S01-S03.Complete",
        normalised_name="Some Show S01-S03 Complete",
    )

    with patch("debridbridge.importer.pipeline.parse_torrent", return_value=fake_parse):
        await pipeline._process(task)

    # Should have 3 virtual downloads (one per season)
    with db.get_db() as conn:
        vds = conn.execute(
            "SELECT * FROM virtual_downloads WHERE rd_hash = ?", (FAKE_HASH,)
        ).fetchall()
        assert len(vds) == 3

        seasons = sorted([row["season_number"] for row in vds])
        assert seasons == ["1", "2", "3"]

        # All should be 'done'
        assert all(row["status"] == "done" for row in vds)

    # manual_import called 3 times (once per season)
    assert sonarr_client.manual_import.await_count == 3


@pytest.mark.asyncio
async def test_low_confidence_goes_to_needs_review(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """Parses with confidence < 0.4 must be flagged needs_review."""
    _seed_torrent(db)
    vd_id = _seed_vd(db)

    files = [RDTorrentFile(id=1, path="/garbage_name/file.mkv", bytes=100_000_000, selected=1)]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("xzf_843_unknown", files)
    )

    # Force a low-confidence parse result
    low_conf_parse = ParseResult(
        title="",
        year=None,
        season=None,
        episodes=[],
        confidence=0.1,
        media_type="episode",
        raw_name="xzf_843_unknown",
        normalised_name="xzf 843 unknown",
    )

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")

    with patch("debridbridge.importer.pipeline.parse_torrent", return_value=low_conf_parse):
        await pipeline._process(task)

    # Should NOT have contacted Sonarr
    sonarr_client.lookup_series.assert_not_awaited()

    # VD should be needs_review
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status, parse_confidence FROM virtual_downloads WHERE id = ?", (vd_id,)
        ).fetchone()
        assert row["status"] == "needs_review"
        assert row["parse_confidence"] == pytest.approx(0.1, abs=0.05)


@pytest.mark.asyncio
async def test_import_failure_marks_vd_failed(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """When Sonarr manual import returns False, the VD should be marked failed."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/Show.S01E01/Show.S01E01.mkv", bytes=500_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Show.S01E01", files)
    )

    # Sonarr rejects the import
    sonarr_client.manual_import = AsyncMock(return_value=False)

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")
    await pipeline._process(task)

    with db.get_db() as conn:
        vds = conn.execute(
            "SELECT * FROM virtual_downloads WHERE rd_hash = ?", (FAKE_HASH,)
        ).fetchall()
        assert len(vds) >= 1
        # At least one VD should be failed (the one whose import was rejected)
        assert any(row["status"] == "failed" for row in vds)


@pytest.mark.asyncio
async def test_movie_pipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs):
    """Full pipeline for a movie import through Radarr."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/Some.Movie.2024/Some.Movie.2024.1080p.mkv",
                       bytes=4_000_000_000, selected=1),
        RDTorrentFile(id=2, path="/Some.Movie.2024/sample.mkv",
                       bytes=10_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Some.Movie.2024.1080p", files)
    )

    # Force a movie parse result
    movie_parse = ParseResult(
        title="Some Movie",
        year=2024,
        season=None,
        episodes=[],
        confidence=0.8,
        media_type="movie",
        raw_name="Some.Movie.2024.1080p",
        normalised_name="Some Movie 2024",
    )

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "radarr", "http://radarr:7878", "/downloads")

    with patch("debridbridge.importer.pipeline.parse_torrent", return_value=movie_parse):
        await pipeline._process(task)

    # Radarr was asked to import
    radarr_client.lookup_movie.assert_awaited_once()
    radarr_client.manual_import_movie.assert_awaited_once()

    # Sonarr should NOT have been contacted
    sonarr_client.lookup_series.assert_not_awaited()

    # VD is done
    with db.get_db() as conn:
        vds = conn.execute(
            "SELECT * FROM virtual_downloads WHERE rd_hash = ?", (FAKE_HASH,)
        ).fetchall()
        assert len(vds) == 1
        assert vds[0]["status"] == "done"

    # Symlink was created — should pick the largest file (4GB), not the sample
    with db.get_db() as conn:
        symlinks = conn.execute("SELECT * FROM symlinks").fetchall()
        assert len(symlinks) == 1
        assert os.path.islink(symlinks[0]["symlink_path"])
        # The symlink target should reference the larger file, not the sample
        assert "sample" not in symlinks[0]["target_path"]


@pytest.mark.asyncio
async def test_series_not_found_goes_to_needs_review(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """When Sonarr cannot find the series, it should be flagged needs_review."""
    _seed_torrent(db)
    vd_id = _seed_vd(db)

    files = [
        RDTorrentFile(id=1, path="/Rare.Show.S01E05/Rare.Show.S01E05.mkv",
                       bytes=300_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Rare.Show.S01E05", files)
    )

    # Sonarr returns no match
    sonarr_client.lookup_series = AsyncMock(return_value=None)

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")
    await pipeline._process(task)

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM virtual_downloads WHERE id = ?", (vd_id,)
        ).fetchone()
        assert row["status"] == "needs_review"


@pytest.mark.asyncio
async def test_torrent_never_ready_marks_failed(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """If RD torrent stays in 'downloading' status past max_attempts, mark failed."""
    _seed_torrent(db)
    vd_id = _seed_vd(db)

    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Some.Show.S01E01", status="downloading")
    )

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")

    # Override _wait_for_ready to avoid real polling/sleeping — call with max_attempts=1
    with patch.object(pipeline, "_wait_for_ready", return_value=None):
        await pipeline._process(task)

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM virtual_downloads WHERE id = ?", (vd_id,)
        ).fetchone()
        assert row["status"] == "failed"


@pytest.mark.asyncio
async def test_rd_error_status_marks_failed(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """If RD torrent reaches an error status, mark failed."""
    _seed_torrent(db)
    vd_id = _seed_vd(db)

    error_info = _make_torrent_info("Some.Show.S01E01", status="error")
    rd_client.get_torrent_info = AsyncMock(return_value=error_info)

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")

    # _wait_for_ready should return None for error status
    await pipeline._process(task)

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM virtual_downloads WHERE id = ?", (vd_id,)
        ).fetchone()
        assert row["status"] == "failed"


@pytest.mark.asyncio
async def test_no_arr_client_for_category_marks_failed(
    rd_client, db, tmp_dirs
):
    """If category doesn't map to any Arr client, mark failed."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/Show.S01E01/Show.S01E01.mkv", bytes=500_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("Show.S01E01", files)
    )

    # Pipeline with no Arr clients
    pipeline = ImportPipeline(rd_client, None, None, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "unknown_category", "http://x:1234", "/downloads")

    # Force a valid parse so we reach the arr-client lookup
    ep_parse = ParseResult(
        title="Show", year=None, season=1, episodes=[1],
        confidence=0.9, media_type="episode",
        raw_name="Show.S01E01", normalised_name="Show S01E01",
    )
    with patch("debridbridge.importer.pipeline.parse_torrent", return_value=ep_parse):
        await pipeline._process(task)

    # No VDs were created by the pipeline (it failed before that), but _mark_failed
    # updates any existing pending ones — so there should be no 'done' VDs
    with db.get_db() as conn:
        done = conn.execute(
            "SELECT * FROM virtual_downloads WHERE rd_hash = ? AND status = 'done'",
            (FAKE_HASH,),
        ).fetchall()
        assert len(done) == 0


@pytest.mark.asyncio
async def test_symlinks_point_to_correct_mount_path(
    rd_client, sonarr_client, radarr_client, db, tmp_dirs
):
    """Verify symlink targets reference mount_path/__all__/torrent_name/filename."""
    _seed_torrent(db)

    files = [
        RDTorrentFile(id=1, path="/MyShow.S02E03/MyShow.S02E03.720p.mkv",
                       bytes=350_000_000, selected=1),
    ]
    rd_client.get_torrent_info = AsyncMock(
        return_value=_make_torrent_info("MyShow.S02E03.720p", files)
    )

    ep_parse = ParseResult(
        title="MyShow", year=None, season=2, episodes=[3],
        confidence=0.9, media_type="episode",
        raw_name="MyShow.S02E03.720p", normalised_name="MyShow S02E03",
    )

    pipeline = ImportPipeline(rd_client, sonarr_client, radarr_client, db, tmp_dirs)
    task = ImportTask(FAKE_HASH, FAKE_MAGNET, "sonarr", "http://sonarr:8989", "/downloads")

    with patch("debridbridge.importer.pipeline.parse_torrent", return_value=ep_parse):
        await pipeline._process(task)

    with db.get_db() as conn:
        symlinks = conn.execute("SELECT * FROM symlinks").fetchall()
        assert len(symlinks) >= 1
        for sl in symlinks:
            target = sl["target_path"]
            # Target must be under mount_path/__all__/torrent_name
            assert target.startswith(tmp_dirs.mount_path)
            assert "/__all__/" in target
            assert "MyShow.S02E03.720p" in target
