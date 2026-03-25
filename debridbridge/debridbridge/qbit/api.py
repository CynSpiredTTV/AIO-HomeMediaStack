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
async def auth_login(request: Request):
    """Always succeed — matches Decypharr's handleLogin."""
    response = PlainTextResponse("Ok.")
    response.set_cookie("SID", _SESSION_ID, httponly=True, path="/")
    return response


# --- App info (versions match Decypharr exactly) ---

@router.get("/app/version")
async def app_version():
    return PlainTextResponse("v4.3.2")


@router.get("/app/webapiVersion")
async def webapi_version():
    return PlainTextResponse("2.7")


@router.get("/app/buildInfo")
async def app_build_info():
    return {
        "bitness": 64,
        "boost": "1.75.0",
        "libtorrent": "1.2.11.0",
        "openssl": "1.1.1i",
        "qt": "5.15.2",
        "zlib": "1.2.11",
    }


@router.get("/app/preferences")
async def app_preferences(request: Request):
    """Full qBittorrent preferences — Radarr/Sonarr parse this during connection test."""
    settings = request.app.state.settings
    save_path = f"{settings.symlink_path}/"
    return {
        "add_trackers": "",
        "add_trackers_enabled": False,
        "alt_dl_limit": 10240,
        "alt_up_limit": 10240,
        "alternative_webui_enabled": False,
        "alternative_webui_path": "",
        "announce_ip": "",
        "announce_to_all_tiers": True,
        "announce_to_all_trackers": False,
        "anonymous_mode": False,
        "async_io_threads": 4,
        "auto_delete_mode": 0,
        "auto_tmm_enabled": False,
        "autorun_enabled": False,
        "autorun_program": "",
        "banned_IPs": "",
        "bittorrent_protocol": 0,
        "bypass_auth_subnet_whitelist": "",
        "bypass_auth_subnet_whitelist_enabled": False,
        "bypass_local_auth": False,
        "category_changed_tmm_enabled": False,
        "checking_memory_use": 32,
        "create_subfolder_enabled": True,
        "current_interface_address": "",
        "current_network_interface": "",
        "dht": True,
        "disk_cache": -1,
        "disk_cache_ttl": 60,
        "dl_limit": 0,
        "dont_count_slow_torrents": False,
        "dyndns_domain": "changeme.dyndns.org",
        "dyndns_enabled": False,
        "dyndns_password": "",
        "dyndns_service": 0,
        "dyndns_username": "",
        "embedded_tracker_port": 9000,
        "enable_coalesce_read_write": True,
        "enable_embedded_tracker": False,
        "enable_multi_connections_from_same_ip": False,
        "enable_os_cache": True,
        "enable_piece_extent_affinity": False,
        "enable_super_seeding": False,
        "enable_upload_suggestions": False,
        "encryption": 0,
        "export_dir": "",
        "export_dir_fin": "",
        "file_pool_size": 40,
        "incomplete_files_ext": False,
        "ip_filter_enabled": False,
        "ip_filter_path": "",
        "ip_filter_trackers": False,
        "limit_lan_peers": True,
        "limit_tcp_overhead": False,
        "limit_utp_rate": True,
        "listen_port": 31193,
        "locale": "en",
        "lsd": True,
        "mail_notification_auth_enabled": False,
        "mail_notification_email": "",
        "mail_notification_enabled": False,
        "mail_notification_password": "",
        "mail_notification_sender": "qBittorrentNotification@example.com",
        "mail_notification_smtp": "smtp.changeme.com",
        "mail_notification_ssl_enabled": False,
        "mail_notification_username": "",
        "max_active_downloads": 3,
        "max_active_torrents": 5,
        "max_active_uploads": 3,
        "max_connec": 500,
        "max_connec_per_torrent": 100,
        "max_ratio": -1,
        "max_ratio_act": 0,
        "max_ratio_enabled": False,
        "max_seeding_time": -1,
        "max_seeding_time_enabled": False,
        "max_uploads": -1,
        "max_uploads_per_torrent": -1,
        "outgoing_ports_max": 0,
        "outgoing_ports_min": 0,
        "pex": True,
        "preallocate_all": False,
        "proxy_auth_enabled": False,
        "proxy_ip": "0.0.0.0",
        "proxy_password": "",
        "proxy_peer_connections": False,
        "proxy_port": 8080,
        "proxy_torrents_only": False,
        "proxy_type": 0,
        "proxy_username": "",
        "queueing_enabled": False,
        "random_port": False,
        "recheck_completed_torrents": False,
        "resolve_peer_countries": True,
        "rss_auto_downloading_enabled": False,
        "rss_max_articles_per_feed": 50,
        "rss_processing_enabled": False,
        "rss_refresh_interval": 30,
        "save_path": save_path,
        "save_path_changed_tmm_enabled": False,
        "save_resume_data_interval": 60,
        "scan_dirs": {},
        "schedule_from_hour": 8,
        "schedule_from_min": 0,
        "schedule_to_hour": 20,
        "schedule_to_min": 0,
        "scheduler_days": 0,
        "scheduler_enabled": False,
        "send_buffer_low_watermark": 10,
        "send_buffer_watermark": 500,
        "send_buffer_watermark_factor": 50,
        "slow_torrent_dl_rate_threshold": 2,
        "slow_torrent_inactive_timer": 60,
        "slow_torrent_ul_rate_threshold": 2,
        "socket_backlog_size": 30,
        "start_paused_enabled": False,
        "stop_tracker_timeout": 1,
        "temp_path": save_path + "temp/",
        "temp_path_enabled": False,
        "torrent_changed_tmm_enabled": True,
        "up_limit": 0,
        "upload_choking_algorithm": 1,
        "upload_slots_behavior": 0,
        "upnp": True,
        "upnp_lease_duration": 0,
        "use_https": False,
        "utp_tcp_mixed_mode": 0,
        "web_ui_address": "*",
        "web_ui_ban_duration": 3600,
        "web_ui_clickjacking_protection_enabled": True,
        "web_ui_csrf_protection_enabled": True,
        "web_ui_domain_list": "*",
        "web_ui_host_header_validation_enabled": True,
        "web_ui_https_cert_path": "",
        "web_ui_https_key_path": "",
        "web_ui_max_auth_fail_count": 5,
        "web_ui_port": 8080,
        "web_ui_secure_cookie_enabled": True,
        "web_ui_session_timeout": 3600,
        "web_ui_upnp": False,
        "web_ui_username": "admin",
    }


@router.get("/app/shutdown")
async def app_shutdown():
    return Response(status_code=200)


@router.get("/transfer/info")
async def transfer_info():
    return {
        "connection_status": "connected",
        "dht_nodes": 0,
        "dl_info_data": 0,
        "dl_info_speed": 0,
        "dl_rate_limit": 0,
        "up_info_data": 0,
        "up_info_speed": 0,
        "up_rate_limit": 0,
    }


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

        with db.get_db() as conn:
            # Clean up any old entries for this hash so retries work fresh.
            # Sonarr/Radarr handle blacklisting — we just process what they send.
            conn.execute(
                "DELETE FROM symlinks WHERE virtual_download_id IN "
                "(SELECT id FROM virtual_downloads WHERE rd_hash = ?)",
                (infohash,),
            )
            conn.execute("DELETE FROM virtual_downloads WHERE rd_hash = ?", (infohash,))
            conn.execute("DELETE FROM torrents WHERE rd_hash = ?", (infohash,))

            conn.execute(
                "INSERT INTO torrents "
                "(rd_hash, raw_name, status, added_at) "
                "VALUES (?, ?, 'pending', ?)",
                (infohash, magnet, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO virtual_downloads "
                "(id, rd_hash, arr_category, arr_host, qbit_hash, status, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (str(uuid.uuid4()), infohash, category, arr_host, qbit_hash, now),
            )

        logger.info(
            "Torrent added: hash=%s category=%s",
            infohash[:16], category,
        )

        # Queue for import pipeline (non-blocking)
        importer = getattr(request.app.state, "importer", None)
        if importer:
            await importer.enqueue(infohash, magnet, category, arr_host, savepath)

    return PlainTextResponse("Ok.")


@router.get("/torrents/info")
@router.post("/torrents/info")
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

    return [_row_to_torrent_info(row, settings, db) for row in rows]


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

    info = _row_to_torrent_info(row, settings, db)
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
            # Find all VDs for this hash
            vds = conn.execute(
                "SELECT id FROM virtual_downloads WHERE qbit_hash = ? OR rd_hash = ?",
                (h, h),
            ).fetchall()

            # Remove symlinks from disk if requested
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

            # Clean up DB entirely — Sonarr/Radarr handle blacklisting
            for vd in vds:
                conn.execute("DELETE FROM symlinks WHERE virtual_download_id = ?", (vd["id"],))
            conn.execute(
                "DELETE FROM virtual_downloads WHERE qbit_hash = ? OR rd_hash = ?",
                (h, h),
            )
            conn.execute("DELETE FROM torrents WHERE rd_hash = ?", (h,))

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
        info = _row_to_torrent_info(row, settings, db)
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


# --- Additional endpoints Radarr/Sonarr may call (matching Decypharr) ---

@router.get("/torrents/categories")
@router.post("/torrents/categories")
async def torrents_categories(request: Request):
    settings = request.app.state.settings
    categories = {}
    categories["sonarr"] = {
        "name": "sonarr",
        "savePath": f"{settings.symlink_path}/sonarr/",
    }
    categories["radarr"] = {
        "name": "radarr",
        "savePath": f"{settings.symlink_path}/radarr/",
    }
    return categories


@router.post("/torrents/createCategory")
async def create_category(request: Request):
    return Response(status_code=200)


@router.post("/torrents/setCategory")
async def set_category(request: Request):
    return Response(status_code=200)


@router.get("/torrents/tags")
@router.post("/torrents/tags")
async def torrents_tags():
    return []


@router.post("/torrents/createTags")
async def create_tags():
    return Response(status_code=200)


@router.post("/torrents/addTags")
async def add_tags():
    return Response(status_code=200)


@router.post("/torrents/removeTags")
async def remove_tags():
    return Response(status_code=200)


@router.get("/torrents/pause")
@router.post("/torrents/pause")
async def torrents_pause():
    return Response(status_code=200)


@router.get("/torrents/resume")
@router.post("/torrents/resume")
async def torrents_resume():
    return Response(status_code=200)


@router.get("/torrents/recheck")
@router.post("/torrents/recheck")
async def torrents_recheck():
    return Response(status_code=200)


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


def _row_to_torrent_info(row, settings, db=None) -> dict:
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

    # content_path must be a subfolder/file UNDER save_path.
    # Query the actual symlink path to get the real content location.
    content_path = save_path + name if name else save_path
    if db and status in ("done", "importing"):
        try:
            with db.get_db() as conn:
                sl = conn.execute(
                    "SELECT symlink_path FROM symlinks WHERE virtual_download_id = ? LIMIT 1",
                    (row["id"],),
                ).fetchone()
                if sl:
                    # content_path = the parent directory of the symlink file
                    # e.g. /mnt/symlinks/radarr/The Simpsons Movie (2007)/
                    import os
                    content_path = os.path.dirname(sl["symlink_path"]) + "/"
        except Exception:
            pass

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
