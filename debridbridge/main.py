"""DebridBridge entrypoint — starts all services."""

import asyncio
import collections
import json
import logging
import os
import signal
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from debridbridge.config import load_settings
from debridbridge.database import Database
from debridbridge.ratelimit.limiter import RDRateLimiter
from debridbridge.realdebrid.client import RDClient
from debridbridge.arr.client import ArrClient
from debridbridge.importer.pipeline import ImportPipeline
from debridbridge.repair.worker import RepairWorker
from debridbridge.qbit.api import router as qbit_router
from debridbridge.web.api import router as web_router

logger = logging.getLogger("debridbridge")


class MemoryLogHandler(logging.Handler):
    """Keeps the last N log lines in memory for the web UI."""

    def __init__(self, max_lines: int = 200):
        super().__init__()
        self._lines: collections.deque[str] = collections.deque(maxlen=max_lines)

    def emit(self, record):
        self._lines.append(self.format(record))

    def get_lines(self) -> list[str]:
        return list(self._lines)


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def setup_logging(level: str) -> MemoryLogHandler:
    """Configure structured JSON logging to stdout + in-memory buffer."""
    json_fmt = JSONFormatter()

    # Human-readable format for memory handler (web UI)
    human_fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Stdout handler — JSON formatted (BUG-032 fix)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(json_fmt)

    # Memory handler for web UI — human readable
    mem_handler = MemoryLogHandler(max_lines=200)
    mem_handler.setFormatter(human_fmt)

    root = logging.getLogger()
    # Clear any existing handlers to prevent duplicates when create_app()
    # is called multiple times (uvicorn imports the module then starts it)
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(stdout_handler)
    root.addHandler(mem_handler)

    return mem_handler


async def run_startup_checks(settings, rd_client, sonarr_client, radarr_client):
    """Run all startup health checks. Raises SystemExit on failure."""
    errors = []

    # Check /dev/fuse
    if not os.path.exists("/dev/fuse"):
        logger.warning(
            "FUSE not available (/dev/fuse missing). "
            "rclone mount will not work. Ensure container has /dev/fuse and SYS_ADMIN."
        )
        # Not fatal — allow WebDAV-only mode

    # Check RD API key
    try:
        user = await rd_client.get_user()
        logger.info("Real-Debrid: authenticated as %s (type=%s)", user.username, user.type)
        if user.type != "premium":
            errors.append("Real-Debrid account is not premium. Streaming requires a premium subscription.")
    except Exception as e:
        errors.append(f"Real-Debrid API key invalid or unreachable: {e}")

    # Check Sonarr
    if await sonarr_client.test_connection():
        logger.info("Sonarr: connected at %s", settings.sonarr_host)
    else:
        errors.append(
            f"Sonarr not reachable at {settings.sonarr_host}. "
            f"Check SONARR_HOST and SONARR_API_KEY."
        )

    # Check Radarr
    if await radarr_client.test_connection():
        logger.info("Radarr: connected at %s", settings.radarr_host)
    else:
        errors.append(
            f"Radarr not reachable at {settings.radarr_host}. "
            f"Check RADARR_HOST and RADARR_API_KEY."
        )

    # Check directories
    dir_checks = [
        (settings.mount_path, "/DATA/Media/realdebrid"),
        (settings.symlink_path, "/DATA/Media/symlinks"),
        (os.path.dirname(settings.db_path), "/DATA/Work/debridbridge/data"),
    ]
    for container_path, host_path in dir_checks:
        if not os.path.isdir(container_path):
            errors.append(
                f"Directory does not exist: {container_path} "
                f"(host: {host_path}). "
                f"Create it on the ZimaOS host via the file manager."
            )
        elif not os.access(container_path, os.W_OK):
            errors.append(
                f"Directory not writable: {container_path} "
                f"(host: {host_path}). Check permissions."
            )

    if errors:
        for err in errors:
            logger.error("STARTUP CHECK FAILED: %s", err)
        raise SystemExit(
            "Startup checks failed. Fix the issues above and redeploy.\n"
            + "\n".join(f"  - {e}" for e in errors)
        )


def create_app() -> FastAPI:
    settings = load_settings()
    log_handler = setup_logging(settings.log_level)

    logger.info("Starting DebridBridge v0.1.0")

    # Database
    db = Database(settings.db_path)
    db.init()
    db.start_prune_worker()
    logger.info("Database initialized: %s", settings.db_path)

    # Rate limiter
    limiter = RDRateLimiter(
        max_tokens_per_minute=settings.rd_rate_limit,
        db=db,
    )
    logger.info("Rate limiter initialized: %d tokens/min", settings.rd_rate_limit)

    # Real-Debrid client
    rd_client = RDClient(api_key=settings.rd_api_key, limiter=limiter)

    # Arr clients
    sonarr_client = ArrClient(
        host=settings.sonarr_host,
        api_key=settings.sonarr_api_key,
        db=db,
    )
    radarr_client = ArrClient(
        host=settings.radarr_host,
        api_key=settings.radarr_api_key,
        db=db,
    )

    # Import pipeline
    importer = ImportPipeline(
        rd_client=rd_client,
        sonarr_client=sonarr_client,
        radarr_client=radarr_client,
        db=db,
        settings=settings,
    )

    # Repair worker
    repair_worker = RepairWorker(
        rd_client=rd_client,
        arr_clients={"sonarr": sonarr_client, "radarr": radarr_client},
        db=db,
        limiter=limiter,
        interval_hours=settings.repair_interval_hours,
    )

    # Lifespan context manager (replaces deprecated on_event)
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # --- STARTUP ---
        await run_startup_checks(settings, rd_client, sonarr_client, radarr_client)

        await importer.start(num_workers=settings.import_workers)
        repair_worker.start()

        # Start WebDAV + rclone mount
        try:
            from debridbridge.webdav.server import start_webdav_server
            from debridbridge.mount.manager import MountManager

            loop = asyncio.get_event_loop()
            start_webdav_server(db, rd_client, port=settings.webdav_port, loop=loop)
            logger.info("WebDAV server started on port %d", settings.webdav_port)

            mount_mgr = MountManager(
                mount_path=settings.mount_path,
                webdav_port=settings.webdav_port,
            )
            mount_mgr.preflight_check()
            mount_mgr.start()
            app.state.mount_manager = mount_mgr
            logger.info("rclone mount started at %s", settings.mount_path)
        except RuntimeError as e:
            logger.warning("Mount not available: %s", e)
            logger.warning("Running without FUSE mount (WebDAV-only mode)")
        except Exception:
            logger.exception("Failed to start WebDAV/mount")

        yield  # App is running

        # --- SHUTDOWN ---
        logger.info("Shutting down DebridBridge...")
        app.state.shutting_down = True

        logger.info("Draining import queue (up to 30s)...")
        await importer.stop(timeout=30)
        repair_worker.stop()

        mount_mgr = getattr(app.state, "mount_manager", None)
        if mount_mgr:
            mount_mgr.stop()

        await rd_client.close()
        await sonarr_client.close()
        await radarr_client.close()
        db.close()

    # FastAPI app with lifespan
    app = FastAPI(title="DebridBridge", version="0.1.0", lifespan=lifespan)

    # Shutdown flag
    app.state.shutting_down = False

    # Store shared objects on app state
    app.state.settings = settings
    app.state.db = db
    app.state.limiter = limiter
    app.state.rd_client = rd_client
    app.state.sonarr_client = sonarr_client
    app.state.radarr_client = radarr_client
    app.state.importer = importer
    app.state.repair_worker = repair_worker
    app.state.log_handler = log_handler

    # Mount routers
    app.include_router(qbit_router)
    app.include_router(web_router)

    # Serve static files (web UI)
    static_dir = Path(__file__).parent / "web" / "static"
    app.mount("/ui", StaticFiles(directory=str(static_dir), html=True), name="static")

    @app.get("/health")
    async def health():
        mount_mgr = getattr(app.state, "mount_manager", None)
        return {
            "status": "ok",
            "rd_budget_used": limiter.tokens_used,
            "rd_budget_max": settings.rd_rate_limit,
            "mount_healthy": mount_mgr.is_healthy() if mount_mgr else False,
            "queue_depth": importer.queue_depth,
        }

    @app.get("/")
    async def root():
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/ui/")

    return app


app = create_app()


def main():
    settings = load_settings()
    uvicorn.run(
        "debridbridge.main:app",
        host="0.0.0.0",
        port=8080,
        log_level=settings.log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
