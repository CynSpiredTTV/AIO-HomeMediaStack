"""Import pipeline — orchestrates: parse → lookup → symlink → import."""

import asyncio
import hashlib
import logging
import os
import time
import uuid
from pathlib import Path

from debridbridge.arr.client import ArrClient
from debridbridge.parser.guesser import parse as parse_torrent
from debridbridge.parser.normaliser import sanitise_filename
from debridbridge.parser.patterns import regex_parse
from debridbridge.realdebrid.client import RDClient
from debridbridge.realdebrid.models import RDError

logger = logging.getLogger(__name__)

# Unlimited queue — never reject, always queue
MAX_QUEUE_SIZE = 0  # 0 = unlimited


class ImportTask:
    """A single import job."""

    def __init__(
        self,
        infohash: str,
        magnet: str,
        category: str,
        arr_host: str,
        save_path: str,
    ):
        self.infohash = infohash
        self.magnet = magnet
        self.category = category
        self.arr_host = arr_host
        self.save_path = save_path


class ImportPipeline:
    """Async import pipeline with worker pool."""

    def __init__(
        self,
        rd_client: RDClient,
        sonarr_client: ArrClient | None,
        radarr_client: ArrClient | None,
        db,
        settings,
    ):
        self.rd_client = rd_client
        self.sonarr_client = sonarr_client
        self.radarr_client = radarr_client
        self.db = db
        self.settings = settings
        self._queue: asyncio.Queue[ImportTask] = asyncio.Queue()  # Unlimited
        self._workers: list[asyncio.Task] = []
        self._running = False

    async def start(self, num_workers: int = 3):
        """Start the worker pool."""
        self._running = True
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i), name=f"import-worker-{i}")
            self._workers.append(task)
        logger.info("Import pipeline started with %d workers", num_workers)

    async def stop(self, timeout: int = 30):
        """Stop all workers, drain queue with timeout."""
        self._running = False
        # Wait for queue to drain up to timeout seconds
        if not self._queue.empty():
            logger.info("Waiting up to %ds for import queue to drain (%d items)...",
                        timeout, self._queue.qsize())
            try:
                await asyncio.wait_for(self._queue.join(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Import queue drain timed out, %d items remaining",
                               self._queue.qsize())
        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("Import pipeline stopped")

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    async def enqueue(
        self,
        infohash: str,
        magnet: str,
        category: str,
        arr_host: str,
        save_path: str,
    ):
        """Add an import task to the queue. Never rejects — always queues."""
        task = ImportTask(infohash, magnet, category, arr_host, save_path)
        await self._queue.put(task)
        logger.info("Queued import: %s (depth=%d)", infohash[:16], self._queue.qsize())

    async def _worker(self, worker_id: int):
        """Worker loop — pulls tasks from the queue and processes them.

        Each torrent costs ~4 API tokens. Workers wait for enough headroom
        before picking up the next task, so we never burn tokens we don't have.
        """
        while self._running:
            # Wait for enough API headroom before taking a task
            # Each torrent needs ~5 tokens (addMagnet + selectFiles + 2x info + possible delete)
            while self._running:
                used = self.rd_client.limiter.tokens_used
                limit = self.rd_client.limiter.max_tokens
                if used + 10 < limit:  # Need at least 10 tokens of headroom
                    break
                await asyncio.sleep(1)

            try:
                task = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return

            try:
                logger.info("[worker-%d] Processing %s (queue=%d)", worker_id, task.infohash[:16], self._queue.qsize())
                await self._process(task)
            except RDError as e:
                if e.status_code == 429:
                    logger.warning(
                        "[worker-%d] Rate limited, requeuing %s in 60s",
                        worker_id,
                        task.infohash[:16],
                    )
                    await asyncio.sleep(60)
                    await self.enqueue(
                        task.infohash, task.magnet, task.category,
                        task.arr_host, task.save_path,
                    )
                else:
                    logger.exception("[worker-%d] RD error for %s", worker_id, task.infohash[:16])
                    self._mark_failed(task.infohash, str(e))
            except Exception:
                logger.exception("[worker-%d] Error processing %s", worker_id, task.infohash[:16])
                self._mark_failed(task.infohash, "Unexpected error")
            finally:
                self._queue.task_done()

    async def _process(self, task: ImportTask):
        """Execute the full import pipeline for one torrent.

        Steps:
        1. Add magnet to RD
        2. Select files
        3. Wait for status = downloaded (this IS the cache check now)
        4. Parse torrent name
        5. Create virtual downloads (split if multi-season)
        6. Create symlinks
        7. Trigger Arr manual import

        Note: RD disabled /torrents/instantAvailability (403).
        We now determine cache status by adding the magnet and checking
        if it reaches 'downloaded' quickly. Non-cached torrents will
        time out or show status 'downloading' and get cleaned up.
        """
        # 1. Add magnet
        add_result = await self.rd_client.add_magnet(task.magnet)
        rd_torrent_id = add_result.id

        # Update DB with RD torrent ID
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE torrents SET rd_torrent_id = ? WHERE rd_hash = ?",
                (rd_torrent_id, task.infohash),
            )

        # 2. Select all files
        await self.rd_client.select_files(rd_torrent_id, "all")

        # 3. Wait for status = downloaded (cache check happens here now)
        torrent_info = await self._wait_for_ready(rd_torrent_id)
        if torrent_info is None:
            self._mark_failed(task.infohash, "Torrent never became ready on RD")
            return

        # Update DB
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE torrents SET raw_name = ?, status = 'ready', "
                "file_count = ?, last_checked_at = ? WHERE rd_hash = ?",
                (
                    torrent_info.filename,
                    len(torrent_info.files),
                    time.time(),
                    task.infohash,
                ),
            )

        # 5. Parse
        file_paths = [f.path for f in torrent_info.files if f.selected]
        parse_result = parse_torrent(torrent_info.filename, file_paths)

        logger.info(
            "Parsed '%s': type=%s season=%s episodes=%s confidence=%.1f multi=%s",
            torrent_info.filename,
            parse_result.media_type,
            parse_result.season,
            parse_result.episodes,
            parse_result.confidence,
            parse_result.is_multiseason,
        )

        # Low confidence — flag for review
        if parse_result.confidence < 0.4:
            logger.warning(
                "Low confidence (%.1f) for '%s' — flagging for review",
                parse_result.confidence,
                torrent_info.filename,
            )
            self._mark_needs_review(task.infohash, parse_result.confidence)
            return

        # 6 & 7 & 8. Create virtual downloads, symlinks, trigger import
        if parse_result.media_type == "movie":
            await self._import_movie(task, torrent_info, parse_result)
        elif parse_result.is_multiseason:
            await self._import_multiseason(task, torrent_info, parse_result)
        else:
            await self._import_episode(task, torrent_info, parse_result)

        # Clean up the original "pending" VD created by qbit/api.py torrents_add
        # (it has season_number=NULL and may be orphaned if season-specific VDs were created)
        self._cleanup_original_vd(task.infohash)

    async def _wait_for_ready(self, rd_torrent_id: str):
        """Check if torrent is cached by waiting briefly then checking once.

        Cached torrents on RD go from waiting_files_selection → downloaded
        almost instantly after selectFiles. We wait 2 seconds then check.
        If not downloaded, wait 3 more seconds and check one final time.
        Total: 2 API calls, 5 seconds max. Minimal token usage.

        Non-cached torrents are deleted from RD to avoid cluttering the list.
        """
        # First check after 2 seconds — enough for most cached torrents
        await asyncio.sleep(2)
        info = await self.rd_client.get_torrent_info(rd_torrent_id)

        if info.status == "downloaded":
            return info

        if info.status in ("error", "dead", "magnet_error", "virus"):
            logger.error("RD torrent %s status: %s", rd_torrent_id, info.status)
            return None

        # Second check after 3 more seconds — for slower cached content
        await asyncio.sleep(3)
        info = await self.rd_client.get_torrent_info(rd_torrent_id)

        if info.status == "downloaded":
            return info

        # Not cached — still downloading/queued after 5 seconds
        logger.warning(
            "Torrent %s not cached on RD (status=%s after 5s), removing",
            rd_torrent_id, info.status,
        )
        try:
            await self.rd_client.delete_torrent(rd_torrent_id)
        except Exception:
            logger.debug("Failed to delete uncached torrent from RD", exc_info=True)
        return None

    async def _import_episode(self, task, torrent_info, parse_result):
        """Import a single episode or season pack via manual import."""
        arr_client = self._get_arr_client(task.category)
        if not arr_client:
            self._mark_failed(task.infohash, f"No Arr client for category: {task.category}")
            return

        series = await arr_client.lookup_series(parse_result.title, parse_result.year)
        if not series:
            logger.warning("Series not found in Sonarr: %s", parse_result.title)
            self._mark_needs_review(task.infohash, parse_result.confidence)
            return

        selected_files = [f for f in torrent_info.files if f.selected]
        season = parse_result.season or 1
        created_symlinks = []

        for rd_file in selected_files:
            orig_filename = rd_file.path.rsplit("/", 1)[-1]
            if not _is_media_file(orig_filename):
                continue

            ep_num = _detect_episode_from_filename(orig_filename)
            clean_filename = _format_episode_filename(
                series.title, season, ep_num, orig_filename
            )

            symlink_dir = os.path.join(
                self.settings.symlink_path,
                task.category,
                sanitise_filename(series.title),
                f"Season {season:02d}",
            )
            rel_path = rd_file.path.lstrip("/")
            target_path = os.path.join(
                self.settings.mount_path,
                "__all__",
                torrent_info.filename,
                rel_path,
            )
            symlink_path = os.path.join(symlink_dir, clean_filename)

            _create_symlink(symlink_path, target_path)
            created_symlinks.append(symlink_path)

            vd_id = self._get_or_create_virtual_download(
                task.infohash, task.category, task.arr_host, season, parse_result.confidence
            )
            self._record_symlink(vd_id, symlink_path, target_path, task.arr_host)

        vd_id = self._get_or_create_virtual_download(
            task.infohash, task.category, task.arr_host, season, parse_result.confidence
        )

        # Trigger manual import with file paths (not directory)
        import_success = False
        if parse_result.episodes and created_symlinks:
            episodes = await arr_client.get_episodes(series.id, season)
            episode_ids = [ep.id for ep in episodes if ep.episodeNumber in parse_result.episodes]

            if episode_ids:
                # Pass each symlink file path individually
                for sl_path in created_symlinks:
                    import_success = await arr_client.manual_import(
                        sl_path, series.id, season, episode_ids,
                        download_id=task.infohash,
                    )
                    if not import_success:
                        break
        else:
            # Season pack — import all created symlinks
            import_success = True
            if created_symlinks:
                episodes = await arr_client.get_episodes(series.id, season)
                if episodes:
                    episode_ids = [ep.id for ep in episodes]
                    for sl_path in created_symlinks:
                        result = await arr_client.manual_import(
                            sl_path, series.id, season, episode_ids,
                            download_id=task.infohash,
                        )
                        if not result:
                            import_success = False

        if import_success:
            self._mark_vd_done(vd_id)
        else:
            self._mark_vd_failed(vd_id, "Arr manual import rejected")

    async def _import_multiseason(self, task, torrent_info, parse_result):
        """Import a multi-season pack — creates N virtual downloads."""
        arr_client = self._get_arr_client(task.category)
        if not arr_client:
            self._mark_failed(task.infohash, f"No Arr client for category: {task.category}")
            return

        series = await arr_client.lookup_series(parse_result.title, parse_result.year)
        if not series:
            logger.warning("Series not found for multi-season: %s", parse_result.title)
            self._mark_needs_review(task.infohash, parse_result.confidence)
            return

        for season_num, season_files in parse_result.season_file_map.items():
            if season_num == 0:
                continue  # Skip unassigned files

            season_symlinks = []
            for filepath in season_files:
                filename = filepath.rsplit("/", 1)[-1]
                if not _is_media_file(filename):
                    continue

                ep_num = _detect_episode_from_filename(filename)
                clean_filename = _format_episode_filename(
                    series.title, season_num, ep_num, filename
                )

                symlink_dir = os.path.join(
                    self.settings.symlink_path,
                    task.category,
                    sanitise_filename(series.title),
                    f"Season {season_num:02d}",
                )
                rel_path = filepath.lstrip("/")
                target_path = os.path.join(
                    self.settings.mount_path,
                    "__all__",
                    torrent_info.filename,
                    rel_path,
                )
                symlink_path = os.path.join(symlink_dir, clean_filename)

                _create_symlink(symlink_path, target_path)
                season_symlinks.append(symlink_path)

                vd_id = self._get_or_create_virtual_download(
                    task.infohash, task.category, task.arr_host,
                    season_num, parse_result.confidence,
                )
                self._record_symlink(vd_id, symlink_path, target_path, task.arr_host)

            # Trigger import for this season — pass file paths, not directory
            vd_id = self._get_or_create_virtual_download(
                task.infohash, task.category, task.arr_host,
                season_num, parse_result.confidence,
            )
            episodes = await arr_client.get_episodes(series.id, season_num)
            if episodes and season_symlinks:
                episode_ids = [ep.id for ep in episodes]
                success = True
                for sl_path in season_symlinks:
                    result = await arr_client.manual_import(
                        sl_path, series.id, season_num, episode_ids,
                        download_id=f"{task.infohash}:s{season_num}",
                    )
                    if not result:
                        success = False
                if success:
                    self._mark_vd_done(vd_id)
                else:
                    self._mark_vd_failed(vd_id, f"Arr import failed for season {season_num}")
            else:
                self._mark_vd_done(vd_id)

    async def _import_movie(self, task, torrent_info, parse_result):
        """Import a movie."""
        arr_client = self._get_arr_client(task.category)
        if not arr_client:
            self._mark_failed(task.infohash, f"No Arr client for category: {task.category}")
            return

        movie = await arr_client.lookup_movie(parse_result.title, parse_result.year)
        if not movie:
            logger.warning("Movie not found in Radarr: %s", parse_result.title)
            self._mark_needs_review(task.infohash, parse_result.confidence)
            return

        # Find the main video file (largest)
        selected_files = [f for f in torrent_info.files if f.selected]
        media_files = [f for f in selected_files if _is_media_file(f.path)]
        if not media_files:
            self._mark_failed(task.infohash, "No media files found in torrent")
            return

        main_file = max(media_files, key=lambda f: f.bytes)
        filename = main_file.path.rsplit("/", 1)[-1]

        # Build folder name: "Title (Year)"
        folder_name = sanitise_filename(parse_result.title)
        if parse_result.year:
            folder_name = f"{folder_name} ({parse_result.year})"

        symlink_dir = os.path.join(
            self.settings.symlink_path,
            task.category,
            folder_name,
        )
        target_path = os.path.join(
            self.settings.mount_path,
            "__all__",
            torrent_info.filename,
            filename,
        )
        symlink_path = os.path.join(symlink_dir, sanitise_filename(filename))

        _create_symlink(symlink_path, target_path)

        vd_id = self._get_or_create_virtual_download(
            task.infohash, task.category, task.arr_host, None, parse_result.confidence
        )
        self._record_symlink(vd_id, symlink_path, target_path, task.arr_host)

        # Pass the actual file path, not the directory
        success = await arr_client.manual_import_movie(
            symlink_path, movie.id, download_id=task.infohash,
        )
        if success:
            self._mark_vd_done(vd_id)
        else:
            self._mark_vd_failed(vd_id, "Radarr manual import rejected")

    # --- DB helpers ---

    def _get_arr_client(self, category: str) -> ArrClient | None:
        if "sonarr" in category.lower():
            return self.sonarr_client
        if "radarr" in category.lower():
            return self.radarr_client
        return None

    def _get_or_create_virtual_download(
        self, infohash, category, arr_host, season, confidence
    ) -> str:
        """Get existing or create new virtual download for this hash+season combo.

        For movies/single episodes (season=None), reuse the VD created by torrents_add.
        For multi-season, create new VDs with season-specific qbit_hashes.
        """
        with self.db.get_db() as conn:
            # SQL NULL comparison needs IS NULL, not = NULL
            if season is None:
                existing = conn.execute(
                    "SELECT id FROM virtual_downloads "
                    "WHERE rd_hash = ? AND arr_category = ? AND season_number IS NULL",
                    (infohash, category),
                ).fetchone()
            else:
                existing = conn.execute(
                    "SELECT id FROM virtual_downloads "
                    "WHERE rd_hash = ? AND arr_category = ? AND season_number = ?",
                    (infohash, category, str(season)),
                ).fetchone()

            if existing:
                # Update status to importing
                conn.execute(
                    "UPDATE virtual_downloads SET status = 'importing', "
                    "parse_confidence = ? WHERE id = ?",
                    (confidence, existing["id"]),
                )
                return existing["id"]

            # Create new VD (multi-season per-season entries)
            vd_id = str(uuid.uuid4())
            qbit_hash = hashlib.sha1(
                f"{infohash}:season:{season}".encode()
            ).hexdigest()

            conn.execute(
                "INSERT INTO virtual_downloads "
                "(id, rd_hash, arr_category, arr_host, season_number, "
                "qbit_hash, status, parse_confidence, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'importing', ?, ?)",
                (vd_id, infohash, category, arr_host,
                 str(season) if season else None,
                 qbit_hash, confidence, time.time()),
            )
            return vd_id

    def _record_symlink(self, vd_id, symlink_path, target_path, arr_host):
        with self.db.get_db() as conn:
            # Check if a symlink at this path already exists (from a different torrent)
            existing = conn.execute(
                "SELECT id, virtual_download_id FROM symlinks WHERE symlink_path = ?",
                (symlink_path,),
            ).fetchone()

            if existing:
                if existing["virtual_download_id"] == vd_id:
                    # Same VD, just update the target
                    conn.execute(
                        "UPDATE symlinks SET target_path = ?, last_verified_at = ? WHERE id = ?",
                        (target_path, time.time(), existing["id"]),
                    )
                else:
                    # Different torrent owns this path — overwrite is fine
                    # (e.g., second season pack for a missing episode)
                    logger.info(
                        "Symlink %s reassigned from VD %s to VD %s (overlapping pack)",
                        symlink_path, existing["virtual_download_id"][:8], vd_id[:8],
                    )
                    conn.execute(
                        "UPDATE symlinks SET virtual_download_id = ?, target_path = ?, "
                        "arr_host = ?, health = 'ok', last_verified_at = ? WHERE id = ?",
                        (vd_id, target_path, arr_host, time.time(), existing["id"]),
                    )
            else:
                conn.execute(
                    "INSERT INTO symlinks "
                    "(id, virtual_download_id, symlink_path, target_path, "
                    "arr_host, health, last_verified_at) "
                    "VALUES (?, ?, ?, ?, ?, 'ok', ?)",
                    (str(uuid.uuid4()), vd_id, symlink_path, target_path,
                     arr_host, time.time()),
                )

    def _mark_vd_done(self, vd_id: str):
        """Mark a specific virtual download as done."""
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE virtual_downloads SET status = 'done', imported_at = ? WHERE id = ?",
                (time.time(), vd_id),
            )

    def _mark_vd_failed(self, vd_id: str, reason: str):
        """Mark a specific virtual download as failed."""
        logger.error("Import failed for VD %s: %s", vd_id[:8], reason)
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE virtual_downloads SET status = 'failed' WHERE id = ?",
                (vd_id,),
            )

    def _mark_failed(self, infohash: str, reason: str):
        """Mark ALL virtual downloads for a hash as failed (pre-pipeline failures)."""
        logger.error("Import failed for %s: %s", infohash[:16], reason)
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE virtual_downloads SET status = 'failed' "
                "WHERE rd_hash = ? AND status IN ('pending', 'importing')",
                (infohash,),
            )

    def _cleanup_original_vd(self, infohash: str):
        """Clean up the original VD created by qbit/api.py torrents_add.

        When the pipeline creates season-specific or movie VDs, the original VD
        (with season_number=NULL, qbit_hash=infohash) becomes orphaned.
        If other VDs now exist for this hash, delete the original pending one.
        """
        with self.db.get_db() as conn:
            # Count VDs with non-NULL season or with 'done'/'importing' status
            count = conn.execute(
                "SELECT COUNT(*) as cnt FROM virtual_downloads "
                "WHERE rd_hash = ? AND season_number IS NOT NULL",
                (infohash,),
            ).fetchone()["cnt"]

            if count > 0:
                # Season-specific VDs exist — remove the orphaned original
                conn.execute(
                    "DELETE FROM virtual_downloads "
                    "WHERE rd_hash = ? AND season_number IS NULL AND status = 'pending'",
                    (infohash,),
                )

    def _mark_needs_review(self, infohash: str, confidence: float):
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE virtual_downloads SET status = 'needs_review', "
                "parse_confidence = ? WHERE rd_hash = ? AND status IN ('pending', 'importing')",
                (confidence, infohash),
            )


def _format_episode_filename(
    series_title: str, season: int, episode: int | None, original_filename: str
) -> str:
    """Format episode filename per spec: {Series Title} - S{NN}E{NN}.{ext}"""
    ext = original_filename.rsplit(".", 1)[-1] if "." in original_filename else "mkv"

    if episode is not None:
        formatted = f"{series_title} - S{season:02d}E{episode:02d}.{ext}"
    else:
        # Keep original name for season packs where we can't determine episode
        formatted = original_filename

    return sanitise_filename(formatted)


def _detect_episode_from_filename(filename: str) -> int | None:
    """Try to extract episode number from a filename."""
    season, episode, _ = regex_parse(filename)
    return episode


def _is_media_file(path: str) -> bool:
    """Check if a file path is a media file."""
    ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    return ext in {"mkv", "mp4", "avi", "m4v", "wmv", "flv", "webm", "ts", "m2ts"}


def _create_symlink(symlink_path: str, target_path: str):
    """Create a symlink, handling existing links."""
    os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

    if os.path.islink(symlink_path):
        existing_target = os.readlink(symlink_path)
        if existing_target == target_path:
            return  # Already correct
        logger.warning(
            "Overwriting symlink %s: %s → %s", symlink_path, existing_target, target_path
        )
        os.remove(symlink_path)
    elif os.path.exists(symlink_path):
        logger.warning("File exists at symlink path, removing: %s", symlink_path)
        os.remove(symlink_path)

    os.symlink(target_path, symlink_path)
    logger.debug("Created symlink: %s → %s", symlink_path, target_path)
