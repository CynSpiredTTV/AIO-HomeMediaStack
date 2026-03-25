"""Mock qBittorrent HTTP API — Sonarr/Radarr talk to this as their download client."""

import base64
import hashlib
import logging
import os
import re
import struct
import time
import uuid

from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import PlainTextResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2")

# In-memory session — Sonarr/Radarr expect a SID cookie
_SESSION_ID = uuid.uuid4().hex

# Incremental sync state (thread-safe enough for single-process uvicorn)
_sync_rid = 0


# --- Auth ---

@router.post("/auth/login")
async def auth_login(
    username: str = Form(default=""),
    password: str = Form(default=""),
):
    """Always succeed — no real auth needed."""
    response = PlainTextResponse("Ok.")
    response.set_cookie("SID", _SESSION_ID, httponly=True, path="/")
    return response


# --- App info ---

@router.get("/app/version")
async def app_version():
    return PlainTextResponse("v5.0.0")


@router.get("/app/webapiVersion")
async def webapi_version():
    return PlainTextResponse("2.9.0")  # BUG-017 fix: was 2.9.3


# --- Torrents ---

@router.post("/torrents/add")
async def torrents_add(request: Request):
    """Handle magnet/torrent submission from Sonarr/Radarr."""
    # BUG-034: reject during shutdown
    if getattr(request.app.state, "shutting_down", False):
        return PlainTextResponse("Fails.")

    db = request.app.state.db
    settings = request.app.state.settings
    rd_client = getattr(request.app.state, "rd_client", None)

    form = await request.form()
    urls = form.get("urls", "")
    category = form.get("category", "")
    savepath = form.get("savepath", "")

    # Handle magnet links (newline-separated)
    magnets = [u.strip() for u in str(urls).split("\n") if u.strip()]

    # Handle uploaded .torrent files (BUG-019 fix)
    for key in form:
        value = form[key]
        if hasattr(value, "read"):
            torrent_data = await value.read()
            torrent_hash = _extract_hash_from_torrent_file(torrent_data)
            if torrent_hash:
                # Construct a magnet from the extracted hash
                magnets.append(f"magnet:?xt=urn:btih:{torrent_hash}")
                logger.info("Extracted hash from .torrent file: %s", torrent_hash[:16])
            else:
                logger.warning("Could not extract hash from uploaded .torrent file")

    if not magnets:
        return PlainTextResponse("Fails.")

    # Determine arr_host from category
    if "sonarr" in category.lower():
        arr_host = settings.sonarr_host
    elif "radarr" in category.lower():
        arr_host = settings.radarr_host
    else:
        arr_host = ""

    for magnet in magnets:
        infohash = _extract_hash(magnet)
        if not infohash:
            logger.warning("Could not extract hash from magnet: %s", magnet[:80])
            continue

        qbit_hash = infohash.lower()
        now = time.time()

        # BUG-018 fix: Check RD cache BEFORE inserting
        is_cached = True
        if rd_client:
            try:
                cache_result = await rd_client.check_cache([infohash])
                is_cached = cache_result.get(infohash.lower(), False)
            except Exception:
                logger.exception("Failed to check RD cache for %s", infohash[:16])
                is_cached = False

        initial_status = "pending" if is_cached else "failed"

        with db.get_db() as conn:
            # Check if already exists (BUG-7: avoid duplicate enqueue)
            existing = conn.execute(
                "SELECT status FROM torrents WHERE rd_hash = ?", (infohash,)
            ).fetchone()
            if existing:
                logger.info("Torrent already exists: %s (status=%s)", infohash[:16], existing["status"])
                continue

            conn.execute(
                "INSERT OR IGNORE INTO torrents "
                "(rd_hash, raw_name, status, added_at) "
                "VALUES (?, ?, ?, ?)",
                (infohash, magnet, "pending" if is_cached else "dead", now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO virtual_downloads "
                "(id, rd_hash, arr_category, arr_host, qbit_hash, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), infohash, category, arr_host, qbit_hash, initial_status, now),
            )

        if not is_cached:
            logger.warning("Torrent NOT cached on RD, marking failed: %s", infohash[:16])
            continue

        logger.info(
            "Torrent added: hash=%s category=%s cached=%s",
            infohash[:16], category, is_cached,
        )

        # Queue for import pipeline (non-blocking)
        importer = getattr(request.app.state, "importer", None)
        if importer:
            await importer.enqueue(infohash, magnet, category, arr_host, savepath)

    return PlainTextResponse("Ok.")


@router.get("/torrents/info")
async def torrents_info(request: Request, hashes: str = "", category: str = ""):
    """Return torrent info — Sonarr/Radarr query by hash or category."""
    db = request.app.state.db
    settings = request.app.state.settings

    with db.get_db() as conn:
        if hashes:
            hash_list = hashes.split("|")
            placeholders = ",".join("?" * len(hash_list))
            # BUG-020 fix: also search by rd_hash for multi-season packs
            rows = conn.execute(
                f"SELECT vd.*, t.raw_name, t.file_count "
                f"FROM virtual_downloads vd "
                f"JOIN torrents t ON vd.rd_hash = t.rd_hash "
                f"WHERE vd.qbit_hash IN ({placeholders}) "
                f"OR vd.rd_hash IN ({placeholders})",
                hash_list + hash_list,
            ).fetchall()
        elif category:
            rows = conn.execute(
                "SELECT vd.*, t.raw_name, t.file_count "
                "FROM virtual_downloads vd "
                "JOIN torrents t ON vd.rd_hash = t.rd_hash "
                "WHERE vd.arr_category = ? AND vd.status != 'deleted'",
                (category,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT vd.*, t.raw_name, t.file_count "
                "FROM virtual_downloads vd "
                "JOIN torrents t ON vd.rd_hash = t.rd_hash "
                "WHERE vd.status != 'deleted'"
            ).fetchall()

    return [_row_to_torrent_info(row, settings) for row in rows]


@router.get("/torrents/properties")
async def torrents_properties(request: Request, hash: str = ""):
    """Return properties for a single torrent."""
    db = request.app.state.db
    settings = request.app.state.settings

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT vd.*, t.raw_name, t.file_count "
            "FROM virtual_downloads vd "
            "JOIN torrents t ON vd.rd_hash = t.rd_hash "
            "WHERE vd.qbit_hash = ?",
            (hash,),
        ).fetchone()

    if not row:
        return {}

    info = _row_to_torrent_info(row, settings)
    info["creation_date"] = int(row["created_at"])
    info["addition_date"] = int(row["created_at"])
    info["completion_date"] = int(row["imported_at"] or 0)
    return info


@router.get("/torrents/files")
async def torrents_files(request: Request, hash: str = ""):
    """Return file list for a torrent — used by Sonarr for import matching."""
    db = request.app.state.db

    with db.get_db() as conn:
        vd = conn.execute(
            "SELECT * FROM virtual_downloads WHERE qbit_hash = ?", (hash,)
        ).fetchone()
        if not vd:
            return []

        symlinks = conn.execute(
            "SELECT * FROM symlinks WHERE virtual_download_id = ?", (vd["id"],)
        ).fetchall()

    files = []
    for i, sl in enumerate(symlinks):
        rel_path = sl["symlink_path"]
        files.append({
            "index": i,
            "name": rel_path,
            "size": 0,
            "progress": 1.0 if vd["status"] == "done" else 0.0,
            "priority": 1,
            "is_seed": vd["status"] == "done",
            "piece_range": [0, 0],
            "availability": 1.0,
        })

    return files


@router.post("/torrents/delete")
async def torrents_delete(
    request: Request,
    hashes: str = Form(default=""),
    deleteFiles: str = Form(default="false"),
):
    """Mark torrents as deleted. Does NOT delete from RD."""
    db = request.app.state.db
    hash_list = hashes.split("|")

    for h in hash_list:
        with db.get_db() as conn:
            # Find all VDs: by qbit_hash OR by rd_hash (for multi-season cascade)
            vds = conn.execute(
                "SELECT id FROM virtual_downloads WHERE qbit_hash = ? OR rd_hash = ?",
                (h, h),
            ).fetchall()

            if deleteFiles.lower() == "true":
                for vd in vds:
                    symlinks = conn.execute(
                        "SELECT symlink_path FROM symlinks WHERE virtual_download_id = ?",
                        (vd["id"],),
                    ).fetchall()
                    for sl in symlinks:
                        try:
                            os.remove(sl["symlink_path"])
                        except OSError:
                            pass

            # Mark all related VDs as deleted
            conn.execute(
                "UPDATE virtual_downloads SET status = 'deleted' "
                "WHERE qbit_hash = ? OR rd_hash = ?",
                (h, h),
            )

    return Response(status_code=200)


@router.get("/sync/maindata")
async def sync_maindata(request: Request, rid: int = 0):
    """Sync endpoint — Sonarr polls this frequently."""
    global _sync_rid
    db = request.app.state.db
    settings = request.app.state.settings

    _sync_rid += 1
    is_full = rid == 0

    with db.get_db() as conn:
        rows = conn.execute(
            "SELECT vd.*, t.raw_name, t.file_count "
            "FROM virtual_downloads vd "
            "JOIN torrents t ON vd.rd_hash = t.rd_hash "
            "WHERE vd.status != 'deleted'"
        ).fetchall()

    torrents = {}
    for row in rows:
        info = _row_to_torrent_info(row, settings)
        torrents[row["qbit_hash"]] = info

    categories = {}
    if settings.sonarr_host:
        categories["sonarr"] = {
            "name": "sonarr",
            "savePath": f"{settings.symlink_path}/sonarr/",
        }
    if settings.radarr_host:
        categories["radarr"] = {
            "name": "radarr",
            "savePath": f"{settings.symlink_path}/radarr/",
        }

    result = {
        "rid": _sync_rid,
        "full_update": is_full,
        "torrents": torrents,
        "categories": categories,
        "server_state": {
            "connection_status": "connected",
            "dl_info_speed": 0,
            "up_info_speed": 0,
            "free_space_on_disk": 1099511627776,
        },
    }

    if is_full:
        result["tags"] = []

    return result


# --- Helpers ---

def _extract_hash(magnet: str) -> str | None:
    """Extract infohash from a magnet link."""
    match = re.search(r"urn:btih:([a-fA-F0-9]{40})", magnet)
    if match:
        return match.group(1).lower()

    # Base32 encoded hash (32 chars)
    match = re.search(r"urn:btih:([A-Za-z2-7]{32})", magnet)
    if match:
        try:
            decoded = base64.b32decode(match.group(1).upper())
            return decoded.hex().lower()
        except Exception:
            pass

    return None


def _extract_hash_from_torrent_file(data: bytes) -> str | None:
    """Extract infohash from a .torrent file's bencoded info dict.

    The infohash is the SHA1 of the bencoded 'info' dictionary.
    """
    try:
        # Find the info dict in the bencoded data
        # Look for b'4:info' marker
        info_start = data.find(b"4:infod")
        if info_start == -1:
            info_start = data.find(b"4:info")
            if info_start == -1:
                return None

        # The info dict starts after "4:info"
        info_offset = info_start + 6
        # Find the matching end of the dict by tracking bencode depth
        info_data = _extract_bencode_value(data, info_offset)
        if info_data:
            return hashlib.sha1(info_data).hexdigest().lower()
    except Exception:
        logger.debug("Failed to parse .torrent file", exc_info=True)
    return None


def _extract_bencode_value(data: bytes, offset: int, depth: int = 0) -> bytes | None:
    """Extract a complete bencoded value starting at offset. Returns the raw bytes."""
    if offset >= len(data) or depth > 50:  # BUG-8: recursion limit
        return None

    start = offset
    ch = data[offset:offset + 1]

    if ch == b"d":  # dict
        offset += 1
        while offset < len(data) and data[offset:offset + 1] != b"e":
            # key (string)
            key_val = _extract_bencode_value(data, offset, depth + 1)
            if key_val is None:
                return None
            offset += len(key_val)
            # value
            val = _extract_bencode_value(data, offset, depth + 1)
            if val is None:
                return None
            offset += len(val)
        offset += 1  # skip 'e'
        return data[start:offset]

    elif ch == b"l":  # list
        offset += 1
        while offset < len(data) and data[offset:offset + 1] != b"e":
            val = _extract_bencode_value(data, offset, depth + 1)
            if val is None:
                return None
            offset += len(val)
        offset += 1
        return data[start:offset]

    elif ch == b"i":  # integer
        end = data.index(b"e", offset)
        return data[start:end + 1]

    elif ch and ch[0:1].isdigit():  # string
        colon = data.index(b":", offset)
        length = int(data[offset:colon])
        end = colon + 1 + length
        return data[start:end]

    return None


def _row_to_torrent_info(row, settings) -> dict:
    """Convert a DB row to a qBit torrent info dict."""
    status = row["status"]
    category = row["arr_category"]

    if status == "pending":
        state = "downloading"
        progress = 0.1
    elif status == "importing":
        state = "downloading"
        progress = 0.9
    elif status == "done":
        state = "pausedUP"
        progress = 1.0
    elif status == "failed":
        state = "error"
        progress = 0.0
    elif status == "needs_review":
        state = "downloading"
        progress = 0.5
    else:
        state = "unknown"
        progress = 0.0

    save_path = f"{settings.symlink_path}/{category}/"
    name = row["raw_name"] if row["raw_name"] else ""

    # content_path: Sonarr uses this to find imported files.
    # For completed downloads, point to the actual symlink directory.
    # Query symlinks to find the real content path.
    content_path = save_path + name  # fallback
    if status == "done":
        # Try to determine actual content path from symlinks
        # (This is computed per-call; for performance, could be cached in DB)
        content_path = save_path  # Sonarr will scan the save_path

    return {
        "hash": row["qbit_hash"],
        "name": name,
        "size": 0,
        "progress": progress,
        "dlspeed": 0,
        "upspeed": 0,
        "state": state,
        "category": category,
        "save_path": save_path,
        "content_path": content_path,
        "added_on": int(row["created_at"]),
        "completion_on": int(row["imported_at"] or 0),
        "amount_left": 0 if progress == 1.0 else 1,
        "completed": 0,
        "downloaded": 0,
        "uploaded": 0,
        "ratio": 0,
        "eta": 0,
        "num_seeds": 100 if status == "done" else 0,
        "num_leechs": 0,
        "tags": "",
        "tracker": "",
        "auto_tmm": False,
        "availability": -1.0,
        "total_size": 0,
        "time_active": 0,
        "seeding_time": 0,
    }
