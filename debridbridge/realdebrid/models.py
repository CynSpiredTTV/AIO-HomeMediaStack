"""Pydantic models for Real-Debrid API responses."""

from pydantic import BaseModel, Field


class RDUser(BaseModel):
    id: int
    username: str
    email: str
    type: str  # "premium" for active subscription
    premium: int  # Unix timestamp of expiration
    expiration: str


class RDTorrentFile(BaseModel):
    id: int
    path: str  # Always starts with /
    bytes: int
    selected: int  # 1 = selected, 0 = not


class AddMagnetResponse(BaseModel):
    id: str  # RD internal torrent ID
    uri: str  # URL to torrent info


class TorrentInfo(BaseModel):
    id: str
    filename: str
    original_filename: str | None = None
    hash: str
    bytes: int
    original_bytes: int | None = None
    host: str = ""
    split: int = 0
    progress: float
    status: str  # magnet_error, waiting_files_selection, queued, downloading, downloaded, error, dead
    added: str = ""
    files: list[RDTorrentFile] = Field(default_factory=list)
    links: list[str] = Field(default_factory=list)
    ended: str | None = None
    speed: int = 0
    seeders: int = 0


class UnrestrictLinkResponse(BaseModel):
    id: str
    filename: str
    mimeType: str = ""
    filesize: int
    link: str  # Original restricted link
    host: str = ""
    chunks: int = 0
    crc: int = 0
    download: str  # Direct CDN URL for streaming
    streamable: int = 0


class TorrentListItem(BaseModel):
    id: str
    filename: str
    hash: str
    bytes: int
    host: str = ""
    split: int = 0
    progress: float
    status: str
    added: str = ""
    links: list[str] = Field(default_factory=list)
    ended: str | None = None
    speed: int = 0
    seeders: int = 0


class RDError(Exception):
    """Typed exception for Real-Debrid API errors."""

    def __init__(self, message: str, error_code: int | None = None, status_code: int | None = None):
        self.error_code = error_code
        self.status_code = status_code
        super().__init__(message)
