"""Real-Debrid API client. Every method goes through the rate limiter."""

import logging

import httpx

from debridbridge.ratelimit.limiter import Priority, RDRateLimiter
from debridbridge.realdebrid.models import (
    AddMagnetResponse,
    RDError,
    RDUser,
    TorrentInfo,
    TorrentListItem,
    UnrestrictLinkResponse,
)

logger = logging.getLogger(__name__)

RD_BASE_URL = "https://api.real-debrid.com/rest/1.0"


class RDClient:
    """Async Real-Debrid API client with mandatory rate limiting."""

    def __init__(self, api_key: str, limiter: RDRateLimiter):
        self.api_key = api_key
        self.limiter = limiter
        self._http = httpx.AsyncClient(
            base_url=RD_BASE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0,
        )

    async def _request(
        self,
        method: str,
        path: str,
        priority: Priority = Priority.NORMAL,
        tokens: int = 1,
        retry_on_429: bool = True,
        **kwargs,
    ) -> httpx.Response:
        """Make a rate-limited request to the RD API.

        All RD API calls MUST go through this method.
        """
        await self.limiter.acquire(tokens=tokens, priority=priority)

        try:
            resp = await self._http.request(method, path, **kwargs)
        except httpx.HTTPError as e:
            raise RDError(f"HTTP error calling {method} {path}: {e}") from e

        logger.debug("RD %s %s → %d", method, path, resp.status_code)

        if resp.status_code == 429:
            if retry_on_429:
                await self.limiter.on_429()
                # Retry once after backoff
                await self.limiter.acquire(tokens=tokens, priority=priority)
                try:
                    resp = await self._http.request(method, path, **kwargs)
                except httpx.HTTPError as e:
                    raise RDError(f"HTTP error on retry {method} {path}: {e}") from e

                if resp.status_code == 429:
                    raise RDError(
                        "429 Too Many Requests from RD even after backoff",
                        error_code=34,
                        status_code=429,
                    )
            else:
                raise RDError(
                    "429 Too Many Requests from RD",
                    error_code=34,
                    status_code=429,
                )

        if resp.status_code >= 400:
            error_body = resp.text
            error_code = None
            try:
                data = resp.json()
                error_body = data.get("error", error_body)
                error_code = data.get("error_code")
            except Exception:
                pass
            raise RDError(
                f"RD API error {resp.status_code}: {error_body}",
                error_code=error_code,
                status_code=resp.status_code,
            )

        return resp

    async def get_user(self) -> RDUser:
        """GET /user — verify API key and get account info. Cost: 1 token."""
        resp = await self._request("GET", "/user")
        return RDUser.model_validate(resp.json())

    async def check_cache(self, hashes: list[str]) -> dict[str, bool]:
        """Check instant availability for a list of hashes.

        RD disabled /torrents/instantAvailability (returns 403).
        New approach: add the magnet, select files, check if status becomes
        'downloaded' quickly. If so, it's cached. If not, delete and return False.

        For now, we return True for all hashes and let the pipeline handle
        non-cached torrents during the add→select→wait flow. This avoids
        wasting tokens on a broken endpoint.
        """
        # Always return True — actual cache status is determined in the pipeline
        # when we add_magnet + select_files and check if status reaches 'downloaded'
        return {h.lower(): True for h in hashes}

    async def add_magnet(self, magnet: str) -> AddMagnetResponse:
        """POST /torrents/addMagnet — add a magnet link. Cost: 1 token."""
        resp = await self._request(
            "POST",
            "/torrents/addMagnet",
            data={"magnet": magnet},
        )
        return AddMagnetResponse.model_validate(resp.json())

    async def get_torrent_info(self, torrent_id: str) -> TorrentInfo:
        """GET /torrents/info/{id} — get torrent details. Cost: 1 token."""
        resp = await self._request("GET", f"/torrents/info/{torrent_id}")
        return TorrentInfo.model_validate(resp.json())

    async def select_files(self, torrent_id: str, file_ids: str = "all") -> None:
        """POST /torrents/selectFiles/{id} — select files. Cost: 1 token.

        Args:
            torrent_id: RD internal torrent ID.
            file_ids: "all" or comma-separated file IDs like "1,2,3".
        """
        resp = await self._request(
            "POST",
            f"/torrents/selectFiles/{torrent_id}",
            data={"files": file_ids},
        )
        # 204 No Content on success — no body to parse

    async def unrestrict_link(
        self, link: str, priority: Priority = Priority.CRITICAL
    ) -> UnrestrictLinkResponse:
        """POST /unrestrict/link — get direct download URL. Cost: 1 token.

        Default priority is CRITICAL since this is called on stream access.
        """
        resp = await self._request(
            "POST",
            "/unrestrict/link",
            priority=priority,
            data={"link": link},
        )
        return UnrestrictLinkResponse.model_validate(resp.json())

    async def list_torrents(self, page: int = 1, limit: int = 50) -> list[TorrentListItem]:
        """GET /torrents — list user's torrents. Cost: 1 token per page."""
        resp = await self._request(
            "GET",
            "/torrents",
            priority=Priority.REPAIR,
            params={"page": page, "limit": limit},
        )
        return [TorrentListItem.model_validate(item) for item in resp.json()]

    async def delete_torrent(self, torrent_id: str) -> None:
        """DELETE /torrents/delete/{id} — remove torrent from RD. Cost: 1 token."""
        await self._request(
            "DELETE",
            f"/torrents/delete/{torrent_id}",
            priority=Priority.REPAIR,
        )

    async def close(self):
        await self._http.aclose()
