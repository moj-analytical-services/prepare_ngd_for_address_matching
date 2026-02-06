"""OS Data Hub downloads module.

Handles listing and downloading NGD files from the OS Data Hub API.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from ngd_pipeline.settings import Settings

logger = logging.getLogger(__name__)

# OS Data Hub API base URL
API_BASE_URL = "https://api.os.uk/downloads/v1"

# Download tuning defaults
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB


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


def get_package_version(settings: Settings) -> dict:
    """Fetch package version metadata from the OS Data Hub API.

    Args:
        settings: Application settings containing API credentials and package info.

    Returns:
        Package version metadata dictionary.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    package_id = settings.os_downloads.package_id
    version_id = settings.os_downloads.version_id
    api_key = settings.os_downloads.api_key

    url = f"{API_BASE_URL}/dataPackages/{package_id}/versions/{version_id}"
    headers = {"key": api_key}

    logger.debug("Fetching package metadata from %s", url)
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def list_downloads(metadata: dict) -> list[DownloadItem]:
    """Extract list of downloadable files from package metadata.

    Args:
        metadata: Package version metadata from the API.

    Returns:
        List of DownloadItem objects.
    """
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
    """Print a summary of available downloads.

    Args:
        metadata: Package version metadata.
        items: List of downloadable files.
        api_key: API key for generating download URLs.
    """
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
    session: requests.Session | None = None,
) -> bool:
    """Download a file with streaming and checksum verification.

    Downloads to a .part file first, then atomically renames on success.

    Args:
        url: Download URL.
        dest_path: Destination file path.
        api_key: API key for authentication.
        expected_md5: Expected MD5 hash (optional).
        force: Force download even if file exists.

    Returns:
        True if file was downloaded, False if skipped (already exists).

    Raises:
        requests.HTTPError: If download fails.
        ValueError: If MD5 checksum doesn't match.
    """
    # Skip if file exists and matches expected MD5
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

    # Ensure directory exists
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Download to .part file
    part_path = dest_path.with_suffix(dest_path.suffix + ".part")
    download_url = _add_key_param(url, api_key)

    logger.info("Downloading %s...", dest_path.name)

    sess = session or requests.Session()
    response = sess.get(download_url, stream=True, timeout=(30, 300))
    try:
        response.raise_for_status()

        # Get content length for progress
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

    # Verify MD5 if expected
    if expected_md5:
        actual_md5 = md5_hash.hexdigest()  # type: ignore[union-attr]
        if actual_md5 != expected_md5:
            part_path.unlink(missing_ok=True)
            raise ValueError(
                f"MD5 mismatch for {dest_path.name}: expected {expected_md5}, got {actual_md5}"
            )

    # Atomic rename
    part_path.rename(dest_path)
    logger.info("Downloaded: %s (%s)", dest_path.name, format_size(downloaded))

    return True


def run_download_step(
    settings: Settings,
    force: bool = False,
    list_only: bool = False,
) -> list[Path]:
    """Run the download step of the pipeline.

    Args:
        settings: Application settings.
        force: Force re-download even if files exist.
        list_only: Just list available files, don't download.

    Returns:
        List of downloaded file paths (empty if list_only=True).

    Raises:
        requests.HTTPError: If API requests fail.
    """
    downloads_dir = settings.paths.downloads_dir
    api_key = settings.os_downloads.api_key

    # Fetch package metadata
    logger.info("Fetching package metadata...")
    metadata = get_package_version(settings)

    # Get list of files
    items = list_downloads(metadata)

    if list_only:
        print_download_summary(metadata, items, api_key)
        return []

    # Ensure downloads directory exists
    downloads_dir.mkdir(parents=True, exist_ok=True)

    # Download each file
    downloaded: list[Path] = []
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
        )

        if was_downloaded or dest_path.exists():
            downloaded.append(dest_path)

    logger.info("Download complete: %d file(s)", len(downloaded))
    return downloaded
