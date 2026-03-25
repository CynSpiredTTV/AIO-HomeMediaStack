"""SQLite database management with WAL mode and schema migrations."""

import logging
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 1

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS torrents (
    rd_hash         TEXT PRIMARY KEY,
    rd_torrent_id   TEXT,
    raw_name        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    file_count      INTEGER,
    added_at        REAL NOT NULL,
    last_checked_at REAL
);

CREATE TABLE IF NOT EXISTS virtual_downloads (
    id                  TEXT PRIMARY KEY,
    rd_hash             TEXT NOT NULL REFERENCES torrents(rd_hash),
    arr_category        TEXT NOT NULL,
    arr_host            TEXT NOT NULL,
    season_number       TEXT,
    qbit_hash           TEXT UNIQUE NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    parse_confidence    REAL,
    created_at          REAL NOT NULL,
    imported_at         REAL
);

CREATE TABLE IF NOT EXISTS symlinks (
    id                      TEXT PRIMARY KEY,
    virtual_download_id     TEXT NOT NULL REFERENCES virtual_downloads(id),
    symlink_path            TEXT NOT NULL UNIQUE,
    target_path             TEXT NOT NULL,
    arr_host                TEXT NOT NULL,
    arr_item_id             INTEGER,
    health                  TEXT NOT NULL DEFAULT 'ok',
    last_verified_at        REAL
);

CREATE TABLE IF NOT EXISTS series_cache (
    id              TEXT PRIMARY KEY,
    arr_host        TEXT NOT NULL,
    search_term     TEXT NOT NULL,
    arr_series_id   INTEGER NOT NULL,
    tvdb_id         TEXT,
    tmdb_id         TEXT,
    cached_at       REAL NOT NULL,
    UNIQUE(arr_host, search_term)
);

CREATE TABLE IF NOT EXISTS rd_request_log (
    id              TEXT PRIMARY KEY,
    endpoint        TEXT NOT NULL,
    tokens_used     INTEGER NOT NULL DEFAULT 1,
    requested_at    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rd_request_log_time ON rd_request_log(requested_at);
CREATE INDEX IF NOT EXISTS idx_symlinks_health ON symlinks(health);
CREATE INDEX IF NOT EXISTS idx_virtual_downloads_qbit ON virtual_downloads(qbit_hash);
CREATE INDEX IF NOT EXISTS idx_torrents_status ON torrents(status);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._prune_thread: threading.Thread | None = None
        self._running = False

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        return self._local.conn

    @contextmanager
    def get_db(self):
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def init(self):
        """Create tables and run migrations."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self.get_db() as conn:
            # Check if schema_version table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cursor.fetchone() is None:
                # Fresh database — apply full schema
                conn.executescript(SCHEMA_SQL)
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)",
                    (CURRENT_SCHEMA_VERSION,),
                )
                logger.info("Database initialized at version %d", CURRENT_SCHEMA_VERSION)
            else:
                row = conn.execute("SELECT version FROM schema_version").fetchone()
                current = row["version"] if row else 0
                if current < CURRENT_SCHEMA_VERSION:
                    self._run_migrations(conn, current)
                else:
                    logger.info("Database at version %d, up to date", current)

    def _run_migrations(self, conn: sqlite3.Connection, from_version: int):
        """Run incremental migrations from from_version to CURRENT_SCHEMA_VERSION."""
        # Future migrations go here as elif blocks
        # For now, version 1 is the initial schema — no migrations needed yet
        logger.info(
            "Migrating database from version %d to %d", from_version, CURRENT_SCHEMA_VERSION
        )
        if from_version < 1:
            conn.executescript(SCHEMA_SQL)
        conn.execute(
            "UPDATE schema_version SET version = ?", (CURRENT_SCHEMA_VERSION,)
        )
        logger.info("Database migration complete")

    def start_prune_worker(self):
        """Start background thread that prunes old rd_request_log entries every 60s."""
        self._running = True
        self._prune_thread = threading.Thread(target=self._prune_loop, daemon=True)
        self._prune_thread.start()
        logger.info("Request log prune worker started")

    def stop_prune_worker(self):
        self._running = False
        if self._prune_thread:
            self._prune_thread.join(timeout=5)

    def _prune_loop(self):
        while self._running:
            try:
                cutoff = time.time() - 120  # 2 minutes
                with self.get_db() as conn:
                    result = conn.execute(
                        "DELETE FROM rd_request_log WHERE requested_at < ?", (cutoff,)
                    )
                    if result.rowcount > 0:
                        logger.debug("Pruned %d old request log entries", result.rowcount)
            except Exception:
                logger.exception("Error pruning request log")
            time.sleep(60)

    def close(self):
        self.stop_prune_worker()
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
