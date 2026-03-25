"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    model_config = {"env_prefix": "", "case_sensitive": False}

    # Real-Debrid
    rd_api_key: str = Field(description="Real-Debrid API key")
    rd_rate_limit: int = Field(default=250, description="RD API rate limit per minute (RD allows 250)")

    # Sonarr
    sonarr_host: str = Field(default="http://sonarr:8989", description="Sonarr URL")
    sonarr_api_key: str = Field(description="Sonarr API key")

    # Radarr
    radarr_host: str = Field(default="http://radarr:7878", description="Radarr URL")
    radarr_api_key: str = Field(description="Radarr API key")

    # Paths (inside container)
    mount_path: str = Field(default="/mnt/realdebrid", description="rclone FUSE mount point")
    symlink_path: str = Field(default="/mnt/symlinks", description="Symlink library root")
    db_path: str = Field(default="/data/debridbridge.db", description="SQLite database path")

    # WebDAV
    webdav_port: int = Field(default=8181, description="Internal WebDAV server port")
    webdav_external: bool = Field(default=False, description="Expose WebDAV externally")

    # Repair worker
    repair_interval_hours: int = Field(default=6, description="Hours between repair runs")

    # Import pipeline
    import_workers: int = Field(default=3, description="Concurrent import workers")

    # Logging
    log_level: str = Field(default="INFO", description="Log level")

    # Timezone
    tz: str = Field(default="Europe/London", description="Timezone")


def load_settings() -> Settings:
    """Load and validate settings from environment variables.

    Raises a clear error if required keys are missing.
    """
    try:
        return Settings()
    except Exception as e:
        missing = []
        for field_name in ["rd_api_key", "sonarr_api_key", "radarr_api_key"]:
            import os
            if not os.environ.get(field_name.upper()):
                missing.append(field_name.upper())
        if missing:
            raise SystemExit(
                f"FATAL: Required environment variables not set: {', '.join(missing)}\n"
                f"Set these in your Portainer stack environment or .env file."
            )
        raise
