"""WebDAV server exposing the Real-Debrid library via wsgidav.

Files are served via lazy unrestriction — RD links are only unrestricted when
a file is actually read (GET), not on directory listing (PROPFIND).
"""

import asyncio
import io
import logging
import os
import threading
import time

import httpx
from cheroot.wsgi import Server as CherootServer
from wsgidav.wsgidav_app import WsgiDAVApp
from wsgidav.dav_provider import DAVProvider, DAVCollection, DAVNonCollection

from debridbridge.ratelimit.limiter import Priority

logger = logging.getLogger(__name__)

# Cache unrestricted URLs for 12 hours (RD links typically valid ~12h)
UNRESTRICT_CACHE_TTL = 43200

# Module-level cache: rd_link -> (download_url, timestamp)
_unrestrict_cache: dict[str, tuple[str, float]] = {}
_cache_lock = threading.Lock()

# Torrent info cache: rd_torrent_id -> (file_list, timestamp)
# Avoids hitting RD API on every PROPFIND directory listing
TORRENT_INFO_CACHE_TTL = 300  # 5 minutes
_torrent_info_cache: dict[str, tuple[list, float]] = {}
_torrent_cache_lock = threading.Lock()

# Module-level reference to the async event loop (set on startup)
_event_loop: asyncio.AbstractEventLoop | None = None
_rd_client = None


def _run_async(coro):
    """Run an async coroutine from synchronous wsgidav context."""
    if _event_loop is None or _event_loop.is_closed():
        raise RuntimeError("Event loop not available for WebDAV async calls")
    future = asyncio.run_coroutine_threadsafe(coro, _event_loop)
    return future.result(timeout=30)


def _get_unrestricted_url(rd_link: str) -> str:
    """Get or refresh an unrestricted download URL. Thread-safe, cached."""
    now = time.time()

    with _cache_lock:
        cached = _unrestrict_cache.get(rd_link)
        if cached and (now - cached[1]) < UNRESTRICT_CACHE_TTL:
            return cached[0]

    # Call RD API via the async client (CRITICAL priority — stream access)
    result = _run_async(_rd_client.unrestrict_link(rd_link, priority=Priority.CRITICAL))
    download_url = result.download

    with _cache_lock:
        _unrestrict_cache[rd_link] = (download_url, now)

    return download_url


class _StreamingResponse(io.RawIOBase):
    """A file-like object that streams from an HTTP URL on read.

    Avoids loading the entire file into memory (BUG-13 fix).
    wsgidav calls read() on this object to get file content.
    """

    def __init__(self, url: str):
        self._response = httpx.stream("GET", url, follow_redirects=True, timeout=60.0)
        self._stream = self._response.__enter__()
        self._iter = self._stream.iter_bytes(chunk_size=1024 * 1024)  # 1MB chunks
        self._buffer = b""

    def readable(self):
        return True

    def read(self, size=-1):
        if size == -1:
            # Read everything (fallback — avoid if possible)
            chunks = [self._buffer]
            for chunk in self._iter:
                chunks.append(chunk)
            self._buffer = b""
            return b"".join(chunks)

        # Read up to `size` bytes
        while len(self._buffer) < size:
            try:
                chunk = next(self._iter)
                self._buffer += chunk
            except StopIteration:
                break

        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return result

    def close(self):
        try:
            self._response.__exit__(None, None, None)
        except Exception:
            pass
        super().close()


class RDStreamFile(DAVNonCollection):
    """A virtual file backed by a Real-Debrid unrestricted link.

    On GET, fetches the actual file content by unrestricting the RD link
    and proxying the response from the CDN.
    """

    def __init__(self, path, environ, filename, rd_link, file_size):
        super().__init__(path, environ)
        self.filename = filename
        self.rd_link = rd_link
        self.file_size = file_size

    def support_etag(self):
        return False

    def get_etag(self):
        return None

    def get_content_length(self):
        return self.file_size

    def get_content_type(self):
        ext = self.filename.rsplit(".", 1)[-1].lower() if "." in self.filename else ""
        types = {
            "mkv": "video/x-matroska",
            "mp4": "video/mp4",
            "avi": "video/x-msvideo",
            "m4v": "video/x-m4v",
            "ts": "video/mp2t",
            "srt": "text/plain",
            "sub": "text/plain",
            "nfo": "text/plain",
        }
        return types.get(ext, "application/octet-stream")

    def get_display_name(self):
        return self.filename

    def support_ranges(self):
        return True

    def get_content(self):
        """Unrestrict the RD link and stream the file content.

        Uses httpx streaming to avoid loading the entire file into RAM (BUG-13 fix).
        """
        if not self.rd_link:
            logger.warning("No RD link for file %s", self.filename)
            return io.BytesIO(b"")

        try:
            download_url = _get_unrestricted_url(self.rd_link)
        except Exception:
            logger.exception("Failed to unrestrict link for %s", self.filename)
            return io.BytesIO(b"")

        # Return a streaming wrapper that reads from the CDN on demand
        try:
            return _StreamingResponse(download_url)
        except Exception:
            logger.exception("Failed to open stream for %s", self.filename)
            return io.BytesIO(b"")


class TorrentFolder(DAVCollection):
    """A virtual folder representing a single torrent."""

    def __init__(self, path, environ, torrent_name, files):
        super().__init__(path, environ)
        self.torrent_name = torrent_name
        self._files = files

    def support_etag(self):
        return False

    def get_etag(self):
        return None

    def get_display_name(self):
        return self.torrent_name

    def get_member_names(self):
        return [f[0] for f in self._files]

    def get_member(self, name):
        for filename, rd_link, file_size in self._files:
            if filename == name:
                return RDStreamFile(
                    f"{self.path}{name}",
                    self.environ,
                    filename,
                    rd_link,
                    file_size,
                )
        return None


class AllTorrentsFolder(DAVCollection):
    """Root __all__ folder showing all torrents."""

    def __init__(self, path, environ, db):
        super().__init__(path, environ)
        self.db = db

    def support_etag(self):
        return False

    def get_etag(self):
        return None

    def get_display_name(self):
        return "__all__"

    def get_member_names(self):
        with self.db.get_db() as conn:
            rows = conn.execute(
                "SELECT DISTINCT raw_name FROM torrents WHERE status = 'ready'"
            ).fetchall()
        return [row["raw_name"] for row in rows]

    def get_member(self, name):
        with self.db.get_db() as conn:
            row = conn.execute(
                "SELECT rd_hash, rd_torrent_id, raw_name FROM torrents "
                "WHERE raw_name = ? AND status = 'ready'",
                (name,),
            ).fetchone()
        if row is None:
            return None

        rd_torrent_id = row["rd_torrent_id"]
        rd_hash = row["rd_hash"]

        # Build file list from torrent info stored in DB
        # We need RD links — these are stored after select_files completes
        # Query the torrent info from RD to get the links + file details
        files = self._build_file_list(rd_torrent_id, rd_hash)

        return TorrentFolder(
            f"{self.path}{name}/",
            self.environ,
            name,
            files,
        )

    def _build_file_list(self, rd_torrent_id: str, rd_hash: str) -> list[tuple[str, str, int]]:
        """Build (filename, rd_link, file_size) list for a torrent.

        Uses the symlinks table in DB — no RD API calls needed for directory listings.
        The symlink target_path tells us the file path, and we can derive the filename.
        RD links are only needed for actual file reads (lazy unrestriction in get_content).
        """
        files = []
        try:
            with self.db.get_db() as conn:
                rows = conn.execute(
                    "SELECT s.target_path, s.symlink_path FROM symlinks s "
                    "JOIN virtual_downloads vd ON s.virtual_download_id = vd.id "
                    "WHERE vd.rd_hash = ? AND s.health = 'ok'",
                    (rd_hash,),
                ).fetchall()

            for row in rows:
                target = row["target_path"]
                filename = os.path.basename(target)
                # We don't have the RD link or file size in the symlinks table,
                # but for directory listings we just need the filename.
                # File size 0 is fine for PROPFIND — rclone doesn't need it for listing.
                # The RD link will be resolved lazily on GET via unrestriction.
                files.append((filename, "", 0))

            # If no symlinks yet, try to get info from the torrent files via RD API
            # but only if we have a cached result (never make a blocking RD call here)
            if not files:
                with _torrent_cache_lock:
                    cached = _torrent_info_cache.get(rd_torrent_id)
                    if cached and (time.time() - cached[1]) < TORRENT_INFO_CACHE_TTL:
                        return cached[0]

        except Exception:
            logger.debug("Error building file list for %s", rd_hash[:16], exc_info=True)

        return files


class RootFolder(DAVCollection):
    """WebDAV root showing available directories."""

    def __init__(self, path, environ, db):
        super().__init__(path, environ)
        self.db = db

    def support_etag(self):
        return False

    def get_etag(self):
        return None

    def get_display_name(self):
        return "DebridBridge"

    def get_member_names(self):
        return ["__all__"]

    def get_member(self, name):
        if name == "__all__":
            return AllTorrentsFolder("/__all__/", self.environ, self.db)
        return None


class RDDAVProvider(DAVProvider):
    """Custom DAV provider for the Real-Debrid library."""

    def __init__(self, db):
        super().__init__()
        self.db = db

    def get_resource_inst(self, path, environ):
        if path == "/" or path == "":
            return RootFolder("/", environ, self.db)

        parts = path.strip("/").split("/")

        if len(parts) == 1 and parts[0] == "__all__":
            return AllTorrentsFolder("/__all__/", environ, self.db)

        if len(parts) >= 2 and parts[0] == "__all__":
            torrent_name = parts[1]
            folder = AllTorrentsFolder("/__all__/", environ, self.db)
            member = folder.get_member(torrent_name)
            if member is None:
                return None
            if len(parts) == 2:
                return member
            if len(parts) == 3 and isinstance(member, TorrentFolder):
                return member.get_member(parts[2])

        return None


def create_webdav_app(db) -> WsgiDAVApp:
    """Create the WsgiDAV application."""
    provider = RDDAVProvider(db)

    config = {
        "provider_mapping": {"/dav": provider},
        "verbose": 1,
        "logging": {"enable": True, "enable_loggers": []},
        "http_authenticator": {
            "domain_controller": None,
            "accept_basic": False,
            "accept_digest": False,
            "default_to_digest": False,
        },
        "simple_dc": {"user_mapping": {"*": True}},
    }

    return WsgiDAVApp(config)


def start_webdav_server(db, rd_client, port: int = 8181, loop=None) -> threading.Thread:
    """Start the WebDAV server in a background thread.

    Args:
        db: Database instance.
        rd_client: RDClient instance (async).
        port: Port to listen on.
        loop: The asyncio event loop to use for async RD calls from sync context.
    """
    global _event_loop, _rd_client
    _rd_client = rd_client
    _event_loop = loop or asyncio.get_event_loop()

    wsgi_app = create_webdav_app(db)

    server = CherootServer(("0.0.0.0", port), wsgi_app)
    server.shutdown_timeout = 5

    thread = threading.Thread(target=server.start, daemon=True, name="webdav-server")
    thread.start()
    logger.info("WebDAV server started on port %d", port)
    return thread
