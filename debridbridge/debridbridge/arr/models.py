"""Pydantic models for Sonarr/Radarr API responses."""

from pydantic import BaseModel, Field


class ArrSeries(BaseModel):
    id: int
    title: str
    year: int | None = None
    path: str = ""
    tvdbId: int | None = None
    imdbId: str | None = None
    titleSlug: str = ""


class ArrEpisode(BaseModel):
    id: int
    episodeNumber: int
    seasonNumber: int
    title: str = ""
    episodeFileId: int = 0


class ArrMovie(BaseModel):
    id: int
    title: str
    year: int | None = None
    path: str = ""
    tmdbId: int | None = None
    imdbId: str | None = None
    titleSlug: str = ""


class ArrQuality(BaseModel):
    quality: dict = Field(default_factory=lambda: {"id": 0, "name": "Unknown"})
    revision: dict = Field(
        default_factory=lambda: {"version": 1, "real": 0, "isRepack": False}
    )


class ArrLanguage(BaseModel):
    id: int = 1
    name: str = "English"
