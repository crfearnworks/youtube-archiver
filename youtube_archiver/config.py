import json
import os
from typing import Any, Dict, List, Optional, Union

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load and parse the JSON configuration file.
    Returns a dictionary representing the configuration.
    """
    if not os.path.exists(config_path):
        raise ConfigError(f"Configuration file not found at: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as file:
        try:
            config = json.load(file)
        except json.JSONDecodeError as e:
            raise ConfigError(f"Error parsing config JSON: {e}")

    # Basic validation
    if "channels" not in config or not isinstance(config["channels"], list):
        raise ConfigError("Configuration must contain a 'channels' list.")

    if "default_directories" not in config or not isinstance(config["default_directories"], list):
        raise ConfigError("Configuration must contain a 'default_directories' list.")

    # Optional cookies file path
    config["cookies_file"] = config.get("cookies_file")

    return config


def get_channel_url(channel_identifier: str) -> str:
    """
    Given a channel identifier (URL, channel ID, or handle),
    return a normalized YouTube channel videos URL.
    """
    # Clean up the input first
    channel_identifier = channel_identifier.strip()

    # If it's already a full URL
    if channel_identifier.startswith(("http://", "https://")):
        # Don't process watch URLs - they should be handled separately
        if "youtube.com/watch?v=" in channel_identifier:
            return channel_identifier

        # For channel URLs, ensure we have a trailing '/videos'
        if "/videos" not in channel_identifier:
            return channel_identifier.rstrip("/") + "/videos"
        return channel_identifier

    # Handle channel IDs (UC...)
    if channel_identifier.startswith("UC"):
        return f"https://www.youtube.com/channel/{channel_identifier}/videos"

    # Handle handles (with or without @)
    if "@" in channel_identifier:
        return f"https://www.youtube.com/{channel_identifier.rstrip('/')}/videos"


def get_download_directory(channel_config: Dict[str, Any],
                           default_directories: List[str]) -> str:
    """
    Return the download directory for a given channel config,
    or use one of the default directories if not specified.
    """
    if "download_directory" in channel_config and channel_config["download_directory"]:
        return channel_config["download_directory"]
    else:
        # Fallback to the first default directory if available
        if len(default_directories) > 0:
            return default_directories[0]
        raise ConfigError("No default directory configured.")
