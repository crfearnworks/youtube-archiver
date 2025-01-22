import asyncio
import math
import os
from functools import partial
from typing import Optional, Dict, Any

import yt_dlp
import httpx
from tqdm import tqdm

RETRY_LIMIT = 3

class DownloadError(Exception):
    """Raised when a video fails to download after all retries."""
    pass


async def fetch_video_metadata(url: str) -> Optional[Dict[str, Any]]:
    """
    Fetch metadata for a single video using yt-dlp (Python API), without downloading.
    Returns the metadata dict if successful, or None if an error occurs.
    """
    try:
        # Using run_in_executor to call synchronous yt-dlp code
        loop = asyncio.get_running_loop()
        metadata = await loop.run_in_executor(None, _extract_metadata, url)
        return metadata
    except Exception:
        return None


def _extract_metadata(url: str) -> Dict[str, Any]:
    """Helper function to run the blocking metadata extraction in a separate thread."""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,  # Do not download
        "extract_flat": False
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    return info


async def download_video(video_url: str,
                         download_directory: str,
                         logger,
                         cookies_file: Optional[str] = None, # Added cookies_file here
                         rate_limit: Optional[int] = None) -> None:
    """
    Download a single video using yt-dlp with retries.
    If rate_limit is specified, we sleep (rate_limit) seconds after each download
    to respect potential rate limits.
    """
    tries = 0
    while tries < RETRY_LIMIT:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                partial(_run_yt_dlp_download, video_url, download_directory, logger, cookies_file) # Pass cookies_file
            )
            # Sleep after successful download if rate_limit is set
            if rate_limit:
                await asyncio.sleep(rate_limit)
            return
        except Exception as exc:
            tries += 1
            logger.warning(f"Download attempt {tries} for {video_url} failed: {exc}")
            if tries >= RETRY_LIMIT:
                raise DownloadError(
                    f"Failed to download {video_url} after {RETRY_LIMIT} retries."
                )

def _run_yt_dlp_download(video_url: str, download_directory: str, logger, cookies_file: Optional[str] = None) -> None:
    """Helper function to actually run the blocking yt-dlp download call with desired options."""
    os.makedirs(download_directory, exist_ok=True)

    # YT-DLP options
    ydl_opts = {
        # Ensure videos and metadata are placed in the specified directory
        "outtmpl": os.path.join(download_directory, "%(title)s [%(id)s].%(ext)s"),
        "writesubtitles": True,
        "writeautomaticsub": True,
        "writeinfojson": True,
        "writedescription": True,
        "ignoreerrors": True,
        "quiet": True,  # We'll manage logs ourselves
        "no_warnings": True,
        "continue_dl": True,  # Resume partially downloaded files
        "cachedir": os.path.join(download_directory, ".cache"),
        "merge_output_format": "mkv",  # Force MKV output
        "format": "bestvideo+bestaudio/best",  # Best quality, will be merged into MKV
    }

    if cookies_file:
        ydl_opts["cookiefile"] = cookies_file
        logger.info(f"Using cookies file: {cookies_file}")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        logger.info(f"Starting download: {video_url}")
        ydl.download([video_url])
        logger.info(f"Completed download: {video_url}")


async def download_videos_in_channel(channel_videos: list,
                                     download_dir: str,
                                     logger,
                                     max_concurrent: int = 3,
                                     rate_limit: Optional[int] = None) -> None:
    """
    Download all videos in a channel concurrently, skipping those with 9:16 aspect ratio.
    Uses asyncio gather with a semaphore to limit concurrency.
    Also uses tqdm to display the overall progress.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def sem_download(video_url: str) -> None:
        async with semaphore:
            metadata = await fetch_video_metadata(video_url)
            if metadata is None:
                # If we fail to get metadata, skip gracefully
                logger.warning(f"Skipping download (metadata fetch failed): {video_url}")
                return

            # Now proceed to download
            try:
                await download_video(video_url, download_dir, logger, rate_limit=rate_limit)
            except DownloadError as e:
                logger.error(str(e))

    # We track overall progress by the total number of videos
    with tqdm(total=len(channel_videos), desc="Channel videos", ncols=80) as pbar:
        tasks = []
        for video_url in channel_videos:
            task = asyncio.create_task(sem_download(video_url))
            # Update progress bar upon completion
            task.add_done_callback(lambda _: pbar.update(1))
            tasks.append(task)

        await asyncio.gather(*tasks)
