"""File hashing utilities for duplicate control."""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def sha256_hash(file_path: str | Path) -> str:
    """Generate SHA-256 hash of a file.

    Reads the file in 64KB chunks to handle large PDFs without
    excessive memory usage.

    Args:
        file_path: Path to the file to hash.

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    h = hashlib.sha256()
    path = Path(file_path)

    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)

    digest = h.hexdigest()
    logger.debug("SHA-256 hash for %s: %s", path.name, digest)
    return digest
