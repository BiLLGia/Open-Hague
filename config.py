"""Settings read from the environment (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

VERSION = "0.1.0"


@dataclass(frozen=True)
class Settings:
    database_url: str | None
    data_dir: Path
    contact: str | None
    min_interval: float

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_url=os.environ.get("DATABASE_URL") or None,
            data_dir=Path(os.environ.get("OPEN_HAGUE_DATA_DIR", "data")),
            contact=os.environ.get("OPEN_HAGUE_CONTACT") or None,
            min_interval=float(os.environ.get("OPEN_HAGUE_MIN_INTERVAL", "1.0")),
        )

    @property
    def user_agent(self) -> str:
        if not self.contact:
            return f"OpenHague/{VERSION} (open-source civic data project)"
        return f"OpenHague/{VERSION} (+{self.contact})"
