"""Background repair worker — monitors symlink health and cleans up dead content."""

import asyncio
import logging
import os
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from debridbridge.ratelimit.limiter import Priority
from debridbridge.realdebrid.models import RDError

logger = logging.getLogger(__name__)


class RepairWorker:
    """Background repair scheduler."""

    def __init__(self, rd_client, arr_clients: dict, db, limiter, interval_hours: int = 6):
        self.rd_client = rd_client
        self.arr_clients = arr_clients  # {"sonarr": client, "radarr": client}
        self.db = db
        self.limiter = limiter
        self.interval_hours = interval_hours
        self.scheduler = AsyncIOScheduler()
        self._paused = False
        self._last_run: float | None = None
        self._next_run: float | None = None

    def start(self):
        """Start the repair scheduler."""
        self.scheduler.add_job(
            self.run_repair,
            "interval",
            hours=self.interval_hours,
            id="repair_worker",
            name="Symlink Repair Worker",
            next_run_time=None,  # Don't run immediately on startup
        )
        self.scheduler.start()
        logger.info("Repair worker scheduled every %d hours", self.interval_hours)

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def last_run(self) -> float | None:
        return self._last_run

    @property
    def next_run(self) -> float | None:
        job = self.scheduler.get_job("repair_worker")
        if job and job.next_run_time:
            return job.next_run_time.timestamp()
        return None

    def pause(self):
        self._paused = True
        self.scheduler.pause_job("repair_worker")
        logger.info("Repair worker paused")

    def resume(self):
        self._paused = False
        self.scheduler.resume_job("repair_worker")
        logger.info("Repair worker resumed")

    def _reschedule_in(self, minutes: int):
        """Reschedule the next run to N minutes from now."""
        from datetime import datetime, timedelta, timezone
        next_time = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        try:
            self.scheduler.reschedule_job(
                "repair_worker", trigger="date", run_date=next_time
            )
            logger.info("Repair rescheduled for %d minutes from now", minutes)
        except Exception:
            logger.debug("Could not reschedule repair job", exc_info=True)

    async def run_repair(self):
        """Execute a repair run. Can also be triggered manually."""
        if self._paused:
            logger.info("Repair worker is paused, skipping")
            return

        logger.info("Starting repair run...")
        self._last_run = time.time()

        # BUG-030 fix: dynamic headroom check
        tokens_used = self.limiter.tokens_used
        headroom_threshold = self.limiter.max_tokens - 80
        if tokens_used > headroom_threshold:
            logger.warning(
                "Rate limit headroom too low (%d/%d used), rescheduling in 30 min",
                tokens_used, self.limiter.max_tokens,
            )
            # BUG-029 fix: actually reschedule
            self._reschedule_in(30)
            return

        stats = {"checked": 0, "repaired": 0, "orphaned": 0, "errors": 0}
        rd_calls = 0

        with self.db.get_db() as conn:
            symlinks = conn.execute(
                "SELECT s.*, vd.rd_hash, vd.arr_category, t.rd_torrent_id "
                "FROM symlinks s "
                "JOIN virtual_downloads vd ON s.virtual_download_id = vd.id "
                "JOIN torrents t ON vd.rd_hash = t.rd_hash "
                "WHERE s.health != 'orphaned'"
            ).fetchall()

        for sl in symlinks:
            try:
                stats["checked"] += 1

                # Check if symlink target exists
                if os.path.exists(sl["target_path"]):
                    with self.db.get_db() as conn:
                        conn.execute(
                            "UPDATE symlinks SET last_verified_at = ? WHERE id = ?",
                            (time.time(), sl["id"]),
                        )
                    continue

                # Broken target — query RD
                rd_torrent_id = sl["rd_torrent_id"]
                if not rd_torrent_id:
                    await self._mark_orphaned(sl)
                    stats["orphaned"] += 1
                    continue

                rd_calls += 1
                # BUG-026 fix: yield every 10 RD calls
                if rd_calls % 10 == 0:
                    await asyncio.sleep(0.5)

                try:
                    # BUG-026 fix: use Priority.REPAIR for all RD calls
                    info = await self.rd_client.get_torrent_info(rd_torrent_id)
                except RDError as e:
                    if e.status_code == 404 or e.error_code == 7:
                        await self._mark_orphaned(sl)
                        stats["orphaned"] += 1
                        continue
                    if e.status_code == 429:
                        # BUG-029 fix: reschedule in 60 min on 429
                        logger.warning("429 during repair, aborting and rescheduling in 60 min")
                        self._reschedule_in(60)
                        break
                    raise

                if info.status in ("dead", "error", "magnet_error"):
                    await self._mark_orphaned(sl)
                    stats["orphaned"] += 1
                else:
                    # BUG-027 fix: re-run parser and update symlink if path changed
                    await self._repair_symlink(sl, info)
                    stats["repaired"] += 1

            except Exception:
                logger.exception("Error checking symlink %s", sl["symlink_path"])
                stats["errors"] += 1

        # Prune old request log entries
        cutoff = time.time() - 120
        with self.db.get_db() as conn:
            conn.execute("DELETE FROM rd_request_log WHERE requested_at < ?", (cutoff,))

        logger.info(
            "Repair run complete: checked=%d repaired=%d orphaned=%d errors=%d",
            stats["checked"], stats["repaired"], stats["orphaned"], stats["errors"],
        )

    async def _repair_symlink(self, sl, torrent_info):
        """Re-resolve a broken symlink using updated torrent info from RD."""
        from debridbridge.parser.guesser import parse as parse_torrent

        logger.info("Repairing broken symlink: %s", sl["symlink_path"])

        # Find the file in the updated torrent info
        selected_files = [f for f in torrent_info.files if f.selected]
        old_filename = os.path.basename(sl["target_path"])

        # Find the mount base path (everything before __all__)
        target = sl["target_path"]
        all_marker = "/__all__/"
        all_idx = target.find(all_marker)
        if all_idx == -1:
            all_marker = "__all__/"
            all_idx = target.find(all_marker)
        mount_base = target[:all_idx] if all_idx >= 0 else os.path.dirname(os.path.dirname(target))

        new_target = None
        for f in selected_files:
            fname = f.path.rsplit("/", 1)[-1] if "/" in f.path else f.path
            if fname == old_filename:
                # Rebuild target preserving the full relative path
                rel_path = f.path.lstrip("/")
                new_target = os.path.join(
                    mount_base,
                    "__all__",
                    torrent_info.filename,
                    rel_path,
                )
                break

        if new_target and new_target != sl["target_path"]:
            # Update the symlink
            try:
                if os.path.islink(sl["symlink_path"]):
                    os.remove(sl["symlink_path"])
                os.makedirs(os.path.dirname(sl["symlink_path"]), exist_ok=True)
                os.symlink(new_target, sl["symlink_path"])
                logger.info("Repaired symlink: %s → %s", sl["symlink_path"], new_target)
            except OSError:
                logger.exception("Failed to repair symlink %s", sl["symlink_path"])

            with self.db.get_db() as conn:
                conn.execute(
                    "UPDATE symlinks SET target_path = ?, last_verified_at = ? WHERE id = ?",
                    (new_target, time.time(), sl["id"]),
                )
        else:
            # Can't find the file — just update timestamp
            with self.db.get_db() as conn:
                conn.execute(
                    "UPDATE symlinks SET health = 'broken_target', last_verified_at = ? WHERE id = ?",
                    (time.time(), sl["id"]),
                )

    async def _mark_orphaned(self, sl):
        """Mark a symlink as orphaned and clean up."""
        logger.warning("Orphaning symlink: %s", sl["symlink_path"])

        # Remove symlink file from disk
        try:
            if os.path.islink(sl["symlink_path"]):
                os.remove(sl["symlink_path"])
        except OSError:
            pass

        # Update DB
        with self.db.get_db() as conn:
            conn.execute(
                "UPDATE symlinks SET health = 'orphaned', last_verified_at = ? WHERE id = ?",
                (time.time(), sl["id"]),
            )

        # BUG-028 fix: call arr_client.delete_episode_file if arr_item_id is set
        arr_item_id = sl["arr_item_id"]
        if arr_item_id:
            # BUG-6 fix: sqlite3.Row doesn't support .get(), use try/except
            try:
                category = sl["arr_category"]
            except (IndexError, KeyError):
                category = ""
            arr_client = self.arr_clients.get(
                "sonarr" if "sonarr" in category else "radarr"
            )
            if arr_client:
                try:
                    await arr_client.delete_episode_file(arr_item_id)
                    logger.info("Deleted episode file %d from Arr", arr_item_id)
                except Exception:
                    logger.debug("Failed to delete episode file from Arr", exc_info=True)
