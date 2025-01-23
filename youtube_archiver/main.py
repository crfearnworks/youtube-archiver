import asyncio
import os
from typing import List, Optional, Dict, Any

from youtube_archiver.config import load_config, get_channel_url, get_download_directory
from youtube_archiver.logger import setup_logger
from youtube_archiver.downloader import download_videos_in_channel, download_video, fetch_video_metadata
from youtube_archiver.downloader import DownloadError

import yt_dlp
import httpx


def list_videos_in_channel(channel_url: str):
    """
    Use yt-dlp in a Pythonic way to extract a list of video URLs from the channel's video tab.
    This approach will load the entire video listing in a single call (if possible).
    """
    # If this is a watch URL, return it as a single-item list
    if "youtube.com/watch?v=" in channel_url:
        return [channel_url]

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,  # Flat playlist to just get a list of videos
        "dump_single_json": True
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info_dict = ydl.extract_info(channel_url, download=False)

    entries = info_dict.get("entries", [])
    video_urls = []
    for entry in entries:
        if entry and "id" in entry:  # Changed from 'url' to 'id'
            video_urls.append(f"https://www.youtube.com/watch?v={entry['id']}")
    return video_urls


async def download_video_with_retries(video_url: str,
                                    download_directory: str,
                                    logger,
                                    cookies_file: Optional[str] = None,
                                    rate_limit: Optional[int] = None):
    """
    Downloads a video with retry logic and optional rate limiting.
    """
    try:
        await download_video(video_url, download_directory, logger, cookies_file, rate_limit)
    except DownloadError as e:
        logger.error(f"Failed to download video {video_url} after retries: {e}")
    except Exception as e:
        logger.error(f"Error processing video {video_url}: {e}")


async def process_channel(channel_config: Dict[str, str],
                         default_download_directory: str,
                         logger,
                         semaphore: asyncio.Semaphore,
                         cookies_file: Optional[str] = None,
                         rate_limit: Optional[int] = None):
    """Process a single channel configuration."""
    channel_url = get_channel_url(channel_config["url"])
    download_directory = channel_config.get("download_directory", default_download_directory)

    logger.info(f"Processing channel URL: {channel_url}")

    video_urls = list_videos_in_channel(channel_url)
    logger.info(f"Found {len(video_urls)} videos in channel: {channel_url}")

    tasks = []
    for video_url in video_urls:
        async with semaphore:  # Limit concurrent downloads
            task = asyncio.create_task(
                download_video_with_retries(
                    video_url, download_directory, logger, cookies_file, rate_limit
                )
            )
            tasks.append(task)

    await asyncio.gather(*tasks)
    logger.info(f"Completed processing channel: {channel_url}")


async def main_async(config_path: str, max_concurrent_downloads: int, rate_limit: Optional[int]):
    config = load_config(config_path)
    logger = setup_logger()

    semaphore = asyncio.Semaphore(max_concurrent_downloads)
    default_download_directory = config["default_directories"][0] if config["default_directories"] else "."
    channels_config = config["channels"]
    cookies_file = config.get("cookies_file") # Get cookies_file from config

    logger.info("Starting YouTube Archiver")
    logger.info(f"Maximum concurrent downloads: {max_concurrent_downloads}")
    if rate_limit:
        logger.info(f"Rate limit delay: {rate_limit} seconds after each download")
    if cookies_file:
        logger.info(f"Using cookies file from config: {cookies_file}")

    tasks = []
    for channel_config in channels_config:
        task = asyncio.create_task(
            process_channel(channel_config, default_download_directory, logger, semaphore, cookies_file, rate_limit) # Pass cookies_file
        )
        tasks.append(task)

    await asyncio.gather(*tasks)

    logger.info("YouTube Archiver finished processing all channels.")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="YouTube Archiver: Download videos from specified channels."
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to the JSON configuration file."
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=3,
        help="Maximum number of concurrent downloads."
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=5, # 5 seconds between 
        help="Number of seconds to sleep after each successful download to avoid rate limits."
    )
    args = parser.parse_args()

    config_path = args.config
    max_concurrent_downloads = args.max_concurrent
    rate_limit = args.rate_limit if args.rate_limit > 0 else None # Ensure rate_limit is None if 0

    asyncio.run(main_async(config_path, max_concurrent_downloads, rate_limit))


if __name__ == "__main__":
    main()