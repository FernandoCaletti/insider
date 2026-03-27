"""PDF downloader for CVM insider trading documents.

Creates an HTTP session with a browser User-Agent and cookie persistence,
visits rad.cvm.gov.br first (anti-scraping warm-up), then downloads PDFs
with rate limiting and retries.
"""

import http.cookiejar
import logging
import os
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# Browser-like User-Agent to avoid bot detection.
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Anti-scraping warm-up URL.
_WARMUP_URL = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx"

# Download settings.
DELAY_BETWEEN_REQUESTS = 2  # seconds
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3


def create_session() -> urllib.request.OpenerDirector:
    """Create an HTTP session with cookie persistence and browser User-Agent.

    The session maintains cookies across requests, which is required
    by the CVM portal's anti-scraping measures.

    Returns:
        Configured urllib OpenerDirector.
    """
    cookie_jar = http.cookiejar.CookieJar()
    cookie_handler = urllib.request.HTTPCookieProcessor(cookie_jar)
    opener = urllib.request.build_opener(cookie_handler)
    opener.addheaders = [("User-Agent", _USER_AGENT)]
    return opener


def warmup_session(session: urllib.request.OpenerDirector) -> None:
    """Visit rad.cvm.gov.br to establish cookies before downloading.

    Some CVM endpoints reject requests without a prior visit to the
    portal. This performs a GET to establish the session.

    Args:
        session: The HTTP session to warm up.
    """
    logger.info("Warming up session at %s", _WARMUP_URL)
    try:
        with session.open(_WARMUP_URL, timeout=REQUEST_TIMEOUT) as response:
            response.read()
        logger.info("Session warm-up successful")
    except Exception:
        logger.warning("Session warm-up failed, continuing anyway", exc_info=True)


def download_pdf(
    session: urllib.request.OpenerDirector,
    url: str,
    max_retries: int = MAX_RETRIES,
    timeout: int = REQUEST_TIMEOUT,
) -> str:
    """Download a single PDF to a temporary file.

    Retries on failure with exponential backoff. The caller is responsible
    for deleting the temporary file after processing.

    Args:
        session: HTTP session with cookies.
        url: URL of the PDF to download.
        max_retries: Maximum number of retry attempts.
        timeout: Request timeout in seconds.

    Returns:
        Path to the downloaded temporary file.

    Raises:
        urllib.error.URLError: If all retries are exhausted.
    """
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug("Downloading PDF (attempt %d/%d): %s", attempt, max_retries, url)
            with session.open(url, timeout=timeout) as response:
                content: bytes = response.read()

            # Write to a temp file in /tmp.
            fd, tmp_path = tempfile.mkstemp(suffix=".pdf", dir="/tmp")
            try:
                os.write(fd, content)
            finally:
                os.close(fd)

            logger.info(
                "Downloaded PDF (%d bytes) -> %s",
                len(content),
                tmp_path,
            )
            return tmp_path

        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            last_error = exc
            logger.warning(
                "Download attempt %d/%d failed for %s: %s",
                attempt,
                max_retries,
                url,
                exc,
            )
            if attempt < max_retries:
                backoff = 2 ** attempt
                logger.info("Retrying in %ds...", backoff)
                time.sleep(backoff)

    raise urllib.error.URLError(
        f"Failed to download {url} after {max_retries} attempts: {last_error}"
    )


def cleanup_file(path: str) -> None:
    """Delete a temporary file after processing.

    Logs a warning if the file cannot be deleted but does not raise.

    Args:
        path: Path to the file to delete.
    """
    try:
        p = Path(path)
        if p.exists():
            p.unlink()
            logger.debug("Cleaned up temp file: %s", path)
    except OSError:
        logger.warning("Failed to clean up temp file: %s", path, exc_info=True)


def download_pdfs(
    session: urllib.request.OpenerDirector,
    urls: list[str],
    delay: float = DELAY_BETWEEN_REQUESTS,
) -> list[tuple[str, str | None]]:
    """Download multiple PDFs with rate limiting.

    Downloads PDFs sequentially with a delay between each request.
    Failed downloads are logged but do not stop the batch.

    Args:
        session: HTTP session with cookies.
        urls: List of PDF URLs to download.
        delay: Seconds to wait between requests.

    Returns:
        List of (url, tmp_path) tuples. tmp_path is None if download failed.
    """
    results: list[tuple[str, str | None]] = []

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(delay)

        try:
            tmp_path = download_pdf(session, url)
            results.append((url, tmp_path))
        except Exception:
            logger.error("Failed to download %s", url, exc_info=True)
            results.append((url, None))

    successful = sum(1 for _, p in results if p is not None)
    logger.info(
        "Downloaded %d/%d PDFs successfully",
        successful,
        len(urls),
    )
    return results
