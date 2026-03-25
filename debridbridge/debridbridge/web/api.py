"""Internal REST API for the web UI."""

import logging
import time

from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal")


@router.get("/status")
async def status(request: Request):
    """Rate limiter usage, queue depth, mount health."""
    limiter = request.app.state.limiter
    settings = request.app.state.settings
    importer = getattr(request.app.state, "importer", None)
    mount_mgr = getattr(request.app.state, "mount_manager", None)

    return {
        "rd_budget_used": limiter.tokens_used,
        "rd_budget_max": settings.rd_rate_limit,
        "queue_depth": importer.queue_depth if importer else 0,
        "mount_healthy": mount_mgr.is_healthy() if mount_mgr else False,
    }


@router.get("/torrents")
async def list_torrents(request: Request):
    """List all torrents with status."""
    db = request.app.state.db

    with db.get_db() as conn:
        rows = conn.execute(
            "SELECT t.*, "
            "(SELECT COUNT(*) FROM virtual_downloads vd WHERE vd.rd_hash = t.rd_hash) as vd_count, "
            "(SELECT GROUP_CONCAT(vd2.status, ',') FROM virtual_downloads vd2 WHERE vd2.rd_hash = t.rd_hash) as vd_statuses "
            "FROM torrents t ORDER BY t.added_at DESC LIMIT 100"
        ).fetchall()

    return [
        {
            "rd_hash": row["rd_hash"],
            "raw_name": row["raw_name"],
            "status": row["status"],
            "file_count": row["file_count"],
            "added_at": row["added_at"],
            "virtual_downloads": row["vd_count"],
            "vd_statuses": row["vd_statuses"],
        }
        for row in rows
    ]


@router.get("/needs-review")
async def needs_review(request: Request):
    """List parse results with low confidence."""
    db = request.app.state.db

    with db.get_db() as conn:
        rows = conn.execute(
            "SELECT vd.*, t.raw_name FROM virtual_downloads vd "
            "JOIN torrents t ON vd.rd_hash = t.rd_hash "
            "WHERE vd.status = 'needs_review' "
            "ORDER BY vd.created_at DESC"
        ).fetchall()

    return [
        {
            "id": row["id"],
            "rd_hash": row["rd_hash"],
            "raw_name": row["raw_name"],
            "category": row["arr_category"],
            "confidence": row["parse_confidence"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]


@router.post("/approve/{vd_id}")
async def approve_import(request: Request, vd_id: str):
    """Manually approve a needs_review import."""
    db = request.app.state.db

    with db.get_db() as conn:
        conn.execute(
            "UPDATE virtual_downloads SET status = 'pending' WHERE id = ? AND status = 'needs_review'",
            (vd_id,),
        )

    # Re-enqueue for import
    importer = getattr(request.app.state, "importer", None)
    if importer:
        with db.get_db() as conn:
            vd = conn.execute(
                "SELECT vd.*, t.raw_name FROM virtual_downloads vd "
                "JOIN torrents t ON vd.rd_hash = t.rd_hash "
                "WHERE vd.id = ?",
                (vd_id,),
            ).fetchone()
        if vd:
            # BUG-12 fix: raw_name stores the original magnet link
            magnet = vd["raw_name"] if vd["raw_name"] else ""
            await importer.enqueue(
                vd["rd_hash"], magnet, vd["arr_category"], vd["arr_host"], ""
            )

    return {"status": "approved"}


@router.post("/reject/{vd_id}")
async def reject_import(request: Request, vd_id: str):
    """Reject and remove a needs_review import."""
    db = request.app.state.db

    with db.get_db() as conn:
        conn.execute(
            "UPDATE virtual_downloads SET status = 'rejected' WHERE id = ?",
            (vd_id,),
        )

    return {"status": "rejected"}


@router.get("/symlinks")
async def list_symlinks(request: Request):
    """List all symlinks with health status."""
    db = request.app.state.db

    with db.get_db() as conn:
        # Counts by health
        counts = conn.execute(
            "SELECT health, COUNT(*) as count FROM symlinks GROUP BY health"
        ).fetchall()

        # Broken/orphaned symlinks
        broken = conn.execute(
            "SELECT s.*, t.raw_name FROM symlinks s "
            "JOIN virtual_downloads vd ON s.virtual_download_id = vd.id "
            "JOIN torrents t ON vd.rd_hash = t.rd_hash "
            "WHERE s.health != 'ok' "
            "ORDER BY s.last_verified_at DESC LIMIT 50"
        ).fetchall()

    return {
        "counts": {row["health"]: row["count"] for row in counts},
        "broken": [
            {
                "id": row["id"],
                "symlink_path": row["symlink_path"],
                "target_path": row["target_path"],
                "health": row["health"],
                "raw_name": row["raw_name"],
            }
            for row in broken
        ],
    }


@router.get("/repair/status")
async def repair_status(request: Request):
    """Get repair worker status including last/next run times."""
    repair_worker = getattr(request.app.state, "repair_worker", None)
    if repair_worker:
        return {
            "paused": repair_worker.is_paused,
            "last_run": repair_worker.last_run,
            "next_run": repair_worker.next_run,
        }
    return {"paused": True, "last_run": None, "next_run": None}


@router.post("/repair/run")
async def trigger_repair(request: Request):
    """Trigger an immediate repair run."""
    repair_worker = getattr(request.app.state, "repair_worker", None)
    if repair_worker:
        import asyncio
        asyncio.create_task(repair_worker.run_repair())
        return {"status": "started"}
    return {"status": "repair worker not available"}


@router.post("/repair/pause")
async def pause_repair(request: Request):
    """Pause/resume the repair scheduler."""
    repair_worker = getattr(request.app.state, "repair_worker", None)
    if repair_worker:
        if repair_worker.is_paused:
            repair_worker.resume()
            return {"status": "resumed"}
        else:
            repair_worker.pause()
            return {"status": "paused"}
    return {"status": "repair worker not available"}


@router.get("/logs")
async def get_logs(request: Request):
    """Return last 200 log lines from the in-memory log buffer."""
    handler = getattr(request.app.state, "log_handler", None)
    if handler:
        return {"lines": handler.get_lines()}
    return {"lines": []}
