"""Immutable, content-addressed archive of everything fetched from a source.

Layout: {data_dir}/raw/{source}/{sha256[:2]}/{sha256}{suffix}
Files are written once (atomically) and made read-only, so any later
processing step can be re-run from the exact bytes we received."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

_SOURCE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


@dataclass(frozen=True)
class RawRef:
    sha256: str
    path: Path
    size: int
    created: bool


class RawStore:
    def __init__(self, data_dir: Path) -> None:
        self.root = Path(data_dir) / "raw"

    def put(self, source: str, data: bytes, suffix: str = "") -> RawRef:
        if not _SOURCE_NAME.match(source):
            raise ValueError(f"invalid source name: {source!r}")
        digest = hashlib.sha256(data).hexdigest()
        directory = self.root / source / digest[:2]
        path = directory / f"{digest}{suffix}"
        if path.exists():
            return RawRef(digest, path, len(data), created=False)
        directory.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.chmod(tmp, 0o444)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return RawRef(digest, path, len(data), created=True)
