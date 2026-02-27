from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.os.uk/downloads/v1"
DEFAULT_CHUNK_SIZE = 1024 * 1024 * 20  # 20 MiB
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_READ_TIMEOUT_SECONDS = 300


@dataclass
class DownloadItem:
    """Information about a downloadable file."""

    filename: str
    url: str
    size: int
    md5: str | None


def format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def _add_key_param(url: str, api_key: str) -> str:
    """Add API key as query parameter to URL."""
    parts = urlparse(url)
    params = [
        (k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "key"
    ]
    params.append(("key", api_key))
    return urlunparse(parts._replace(query=urlencode(params)))


def _secret_value(value: Any) -> str:
    """Extract plain value from pydantic SecretStr or plain strings."""
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return str(getter())
    return "" if value is None else str(value)


def _require_api_key(settings: Any) -> str:
    """Read and validate API key from settings object."""
    api_key = _secret_value(getattr(settings.os_downloads, "api_key", None)).strip()
    if not api_key:
        raise ValueError(
            "OS_PROJECT_API_KEY not found in environment. "
            "Create a .env file with OS_PROJECT_API_KEY=<your-key> to use the download step."
        )
    return api_key


def _find_existing_download_archives(downloads_dir: Path) -> list[Path]:
    """Find existing local archives that can be used for extract step."""
    if not downloads_dir.exists():
        return []
    return sorted(downloads_dir.glob("*.zip"))


def get_package_version(settings: Any) -> dict:
    """Fetch package version metadata from the OS Data Hub API."""
    package_id = settings.os_downloads.package_id
    version_id = settings.os_downloads.version_id
    api_key = _require_api_key(settings)

    url = f"{API_BASE_URL}/dataPackages/{package_id}/versions/{version_id}"
    headers = {"key": api_key}
    connect_timeout = getattr(
        settings.os_downloads,
        "connect_timeout_seconds",
        DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )
    read_timeout = getattr(
        settings.os_downloads,
        "read_timeout_seconds",
        DEFAULT_READ_TIMEOUT_SECONDS,
    )

    logger.debug("Fetching package metadata from %s", url)
    response = requests.get(url, headers=headers, timeout=(connect_timeout, read_timeout))
    response.raise_for_status()

    return response.json()


def list_downloads(metadata: dict) -> list[DownloadItem]:
    """Extract list of downloadable files from package metadata."""
    downloads = metadata.get("downloads", [])
    items = []

    for file_info in downloads:
        items.append(
            DownloadItem(
                filename=file_info.get("fileName", "unknown"),
                url=file_info.get("url", ""),
                size=file_info.get("size", 0),
                md5=file_info.get("md5"),
            )
        )

    return items


def print_download_summary(metadata: dict, items: list[DownloadItem], api_key: str) -> None:
    """Print a summary of available downloads."""
    print("=" * 80)
    print(f"Data Package: {metadata.get('dataPackage', {}).get('name', 'N/A')}")
    print(f"Version ID: {metadata.get('id', 'N/A')}")
    print(f"Created: {metadata.get('createdOn', 'N/A')}")
    print(f"Supply Type: {metadata.get('supplyType', 'N/A')}")
    print(f"Format: {metadata.get('format', 'N/A')}")
    print("=" * 80)
    print()

    if not items:
        print("No downloadable files found.")
        return

    print(f"Available Files ({len(items)}):")
    print()

    total_size = 0
    for i, item in enumerate(items, 1):
        total_size += item.size
        download_url = _add_key_param(item.url, api_key) if item.url else "N/A"

        print(f"{i}. {item.filename}")
        print(f"   Size: {format_size(item.size)} ({item.size:,} bytes)")
        print(f"   MD5:  {item.md5 or 'N/A'}")
        print(f"   URL:  {download_url}")
        print()

    print("=" * 80)
    print(f"Total Size: {format_size(total_size)} ({total_size:,} bytes)")
    print("=" * 80)


def _calculate_md5(file_path: Path) -> str:
    """Calculate MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def download_file(
    url: str,
    dest_path: Path,
    api_key: str,
    expected_md5: str | None = None,
    force: bool = False,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    connect_timeout_seconds: int = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    read_timeout_seconds: int = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> bool:
    """Download a file with streaming and checksum verification."""
    if dest_path.exists() and not force:
        if expected_md5:
            actual_md5 = _calculate_md5(dest_path)
            if actual_md5 == expected_md5:
                logger.info("File already exists with matching MD5: %s", dest_path.name)
                return False
            logger.warning("MD5 mismatch for existing file, re-downloading: %s", dest_path.name)
        else:
            logger.info("File already exists: %s", dest_path.name)
            return False

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = dest_path.with_suffix(dest_path.suffix + ".part")
    download_url = _add_key_param(url, api_key)

    logger.info("Downloading %s...", dest_path.name)

    sess = session or requests.Session()
    response = sess.get(
        download_url,
        stream=True,
        timeout=(connect_timeout_seconds, read_timeout_seconds),
    )
    try:
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        next_log = 10 * 1024 * 1024

        md5_hash = hashlib.md5() if expected_md5 else None
        with open(part_path, "wb", buffering=chunk_size) as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                if md5_hash:
                    md5_hash.update(chunk)
                downloaded += len(chunk)

                if total_size and downloaded >= next_log:
                    pct = 100 * downloaded / total_size
                    logger.info(
                        "  Progress: %d/%d MB (%.1f%%)",
                        downloaded // (1024 * 1024),
                        total_size // (1024 * 1024),
                        pct,
                    )
                    next_log += 10 * 1024 * 1024
    finally:
        response.close()

    if expected_md5:
        actual_md5 = md5_hash.hexdigest()  # type: ignore[union-attr]
        if actual_md5 != expected_md5:
            part_path.unlink(missing_ok=True)
            raise ValueError(
                f"MD5 mismatch for {dest_path.name}: expected {expected_md5}, got {actual_md5}"
            )

    part_path.rename(dest_path)
    logger.info("Downloaded: %s (%s)", dest_path.name, format_size(downloaded))

    return True


def run_download_step(
    settings: Any,
    force: bool = False,
    list_only: bool = False,
) -> list[Path]:
    """Run the OS Data Hub download step for any compatible settings object."""
    downloads_dir = settings.paths.downloads_dir

    try:
        api_key = _require_api_key(settings)
    except ValueError as exc:
        if list_only:
            raise

        existing_archives = _find_existing_download_archives(downloads_dir)
        if existing_archives:
            logger.warning(
                "No API key found; using %d existing archive(s) in %s and skipping download.",
                len(existing_archives),
                downloads_dir,
            )
            return existing_archives

        raise ValueError(
            f"{exc} No local zip files were found in {downloads_dir}, so download cannot be skipped."
        ) from exc

    logger.info("Fetching package metadata...")
    metadata = get_package_version(settings)
    items = list_downloads(metadata)

    if list_only:
        print_download_summary(metadata, items, api_key)
        return []

    downloads_dir.mkdir(parents=True, exist_ok=True)

    connect_timeout = getattr(
        settings.os_downloads,
        "connect_timeout_seconds",
        DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )
    read_timeout = getattr(
        settings.os_downloads,
        "read_timeout_seconds",
        DEFAULT_READ_TIMEOUT_SECONDS,
    )

    session = requests.Session()
    downloaded: list[Path] = []
    try:
        for item in items:
            if not item.url:
                logger.warning("No URL for %s, skipping", item.filename)
                continue

            dest_path = downloads_dir / item.filename
            was_downloaded = download_file(
                url=item.url,
                dest_path=dest_path,
                api_key=api_key,
                expected_md5=item.md5,
                force=force,
                connect_timeout_seconds=connect_timeout,
                read_timeout_seconds=read_timeout,
                session=session,
            )

            if was_downloaded or dest_path.exists():
                downloaded.append(dest_path)
    finally:
        session.close()

    logger.info("Download complete: %d file(s)", len(downloaded))
    return downloaded
