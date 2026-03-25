"""Sonarr + Radarr API client."""

import logging
import time
import uuid

import httpx

from debridbridge.arr.models import ArrEpisode, ArrMovie, ArrSeries

logger = logging.getLogger(__name__)


class ArrClient:
    """Client for Sonarr or Radarr API v3."""

    def __init__(self, host: str, api_key: str, db=None):
        self.host = host.rstrip("/")
        self.api_key = api_key
        self.db = db
        self._http = httpx.AsyncClient(
            base_url=self.host,
            headers={"X-Api-Key": api_key},
            timeout=30.0,
        )

    async def test_connection(self) -> bool:
        """Test if the Arr instance is reachable and API key is valid."""
        try:
            resp = await self._http.get("/api/v3/system/status")
            return resp.status_code == 200
        except Exception:
            return False

    # --- Sonarr: Series ---

    async def lookup_series(self, title: str, year: int | None = None) -> ArrSeries | None:
        """Look up a series in Sonarr by title. Uses cache when available."""
        search_term = title.lower().strip()

        # Check cache
        if self.db:
            cached = self._get_cached_series(search_term)
            if cached is not None:
                return cached

        # Query Sonarr
        term = f"{title} {year}" if year else title
        resp = await self._http.get("/api/v3/series/lookup", params={"term": term})
        if resp.status_code != 200:
            logger.warning("Series lookup failed: %d %s", resp.status_code, resp.text[:200])
            return None

        results = resp.json()
        if not results:
            # Try searching existing library
            return await self._search_local_series(search_term)

        # Find the best match — prefer series already in library (id > 0)
        for item in results:
            if item.get("id", 0) > 0:
                series = ArrSeries.model_validate(item)
                self._cache_series(search_term, series)
                return series

        # Fall back to first result
        series = ArrSeries.model_validate(results[0])
        if series.id > 0:
            self._cache_series(search_term, series)
        return series if series.id > 0 else None

    async def _search_local_series(self, search_term: str) -> ArrSeries | None:
        """Search the existing Sonarr library."""
        resp = await self._http.get("/api/v3/series")
        if resp.status_code != 200:
            return None

        for item in resp.json():
            if search_term in item.get("title", "").lower():
                series = ArrSeries.model_validate(item)
                self._cache_series(search_term, series)
                return series
        return None

    async def get_episodes(self, series_id: int, season_number: int) -> list[ArrEpisode]:
        """Get episodes for a series/season from Sonarr."""
        resp = await self._http.get(
            "/api/v3/episode",
            params={"seriesId": series_id, "seasonNumber": season_number},
        )
        if resp.status_code != 200:
            logger.warning("Episode lookup failed: %d", resp.status_code)
            return []

        return [ArrEpisode.model_validate(ep) for ep in resp.json()]

    async def manual_import(
        self,
        file_path: str,
        series_id: int,
        season_number: int,
        episode_ids: list[int],
        download_id: str = "",
    ) -> bool:
        """Trigger manual import in Sonarr.

        POST /api/v3/command with ManualImport.
        """
        payload = {
            "name": "ManualImport",
            "files": [
                {
                    "path": file_path,
                    "seriesId": series_id,
                    "seasonNumber": season_number,
                    "episodeIds": episode_ids,
                    "quality": {
                        "quality": {"id": 0, "name": "Unknown"},
                        "revision": {"version": 1, "real": 0, "isRepack": False},
                    },
                    "languages": [{"id": 1, "name": "English"}],
                    "releaseGroup": "",
                    "downloadId": download_id,
                    "indexerFlags": 0,
                }
            ],
            "importMode": "copy",
        }

        resp = await self._http.post("/api/v3/command", json=payload)
        if resp.status_code in (200, 201):
            logger.info("Manual import triggered for %s", file_path)
            return True

        logger.error("Manual import failed: %d %s", resp.status_code, resp.text[:500])
        return False

    # --- Radarr: Movies ---

    async def lookup_movie(self, title: str, year: int | None = None) -> ArrMovie | None:
        """Look up a movie in Radarr by title."""
        search_term = title.lower().strip()

        # Check cache
        if self.db:
            cached = self._get_cached_movie(search_term)
            if cached is not None:
                return cached

        term = f"{title} {year}" if year else title
        resp = await self._http.get("/api/v3/movie/lookup", params={"term": term})
        if resp.status_code != 200:
            logger.warning("Movie lookup failed: %d %s", resp.status_code, resp.text[:200])
            return None

        results = resp.json()
        if not results:
            return await self._search_local_movie(search_term)

        for item in results:
            if item.get("id", 0) > 0:
                movie = ArrMovie.model_validate(item)
                self._cache_movie(search_term, movie)
                return movie

        movie = ArrMovie.model_validate(results[0])
        return movie if movie.id > 0 else None

    async def _search_local_movie(self, search_term: str) -> ArrMovie | None:
        """Search the existing Radarr library."""
        resp = await self._http.get("/api/v3/movie")
        if resp.status_code != 200:
            return None

        for item in resp.json():
            if search_term in item.get("title", "").lower():
                movie = ArrMovie.model_validate(item)
                self._cache_movie(search_term, movie)
                return movie
        return None

    async def manual_import_movie(
        self,
        file_path: str,
        movie_id: int,
        download_id: str = "",
    ) -> bool:
        """Trigger manual import in Radarr."""
        payload = {
            "name": "ManualImport",
            "files": [
                {
                    "path": file_path,
                    "movieId": movie_id,
                    "quality": {
                        "quality": {"id": 0, "name": "Unknown"},
                        "revision": {"version": 1, "real": 0, "isRepack": False},
                    },
                    "languages": [{"id": 1, "name": "English"}],
                    "releaseGroup": "",
                    "downloadId": download_id,
                    "indexerFlags": 0,
                }
            ],
            "importMode": "copy",
        }

        resp = await self._http.post("/api/v3/command", json=payload)
        if resp.status_code in (200, 201):
            logger.info("Manual movie import triggered for %s", file_path)
            return True

        logger.error("Manual movie import failed: %d %s", resp.status_code, resp.text[:500])
        return False

    async def delete_episode_file(self, episode_file_id: int) -> None:
        """Delete an episode file from Sonarr."""
        resp = await self._http.delete(f"/api/v3/episodefile/{episode_file_id}")
        if resp.status_code not in (200, 204):
            logger.warning("Delete episode file failed: %d", resp.status_code)

    # --- Cache helpers ---

    def _get_cached_series(self, search_term: str) -> ArrSeries | None:
        if not self.db:
            return None
        with self.db.get_db() as conn:
            row = conn.execute(
                "SELECT * FROM series_cache WHERE arr_host = ? AND search_term = ? "
                "AND cached_at > ?",
                (self.host, search_term, time.time() - 604800),  # 7 day TTL
            ).fetchone()
        if row:
            return ArrSeries(
                id=row["arr_series_id"],
                title=search_term,
                tvdbId=int(row["tvdb_id"]) if row["tvdb_id"] else None,
            )
        return None

    def _cache_series(self, search_term: str, series: ArrSeries):
        if not self.db:
            return
        with self.db.get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO series_cache "
                "(id, arr_host, search_term, arr_series_id, tvdb_id, tmdb_id, cached_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    self.host,
                    search_term,
                    series.id,
                    str(series.tvdbId) if series.tvdbId else None,
                    None,
                    time.time(),
                ),
            )

    def _get_cached_movie(self, search_term: str) -> ArrMovie | None:
        if not self.db:
            return None
        with self.db.get_db() as conn:
            row = conn.execute(
                "SELECT * FROM series_cache WHERE arr_host = ? AND search_term = ? "
                "AND cached_at > ?",
                (self.host, search_term, time.time() - 604800),
            ).fetchone()
        if row:
            return ArrMovie(
                id=row["arr_series_id"],
                title=search_term,
                tmdbId=int(row["tmdb_id"]) if row["tmdb_id"] else None,
            )
        return None

    def _cache_movie(self, search_term: str, movie: ArrMovie):
        if not self.db:
            return
        with self.db.get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO series_cache "
                "(id, arr_host, search_term, arr_series_id, tvdb_id, tmdb_id, cached_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    self.host,
                    search_term,
                    movie.id,
                    None,
                    str(movie.tmdbId) if movie.tmdbId else None,
                    time.time(),
                ),
            )

    async def close(self):
        await self._http.aclose()
