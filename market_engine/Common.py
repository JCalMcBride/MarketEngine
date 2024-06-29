import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Union

import aiohttp
import redis
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_fixed, wait_exponential

# ------------------------------
# Config

# Default config when no config.json file is present
default_config: Dict[Any, Any] = {
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_db": 0,
    "output_dir": "output"
}

config: Dict[Any, Any] = {}

# Check if a config.json file is present, and if so, load it
if os.path.isfile("config.json"):
    with open("config.json", "r") as f:
        config.update(json.load(f))
else:
    # If no config.json file is present, use the default config
    config.update(default_config)


# ------------------------------
# Cache Functions

@asynccontextmanager
async def cache_manager():
    """
    Context manager for the Redis cache.
    :return: The Redis cache.
    """
    cache = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])
    yield cache


def get_item_id(item_name: str, item_ids: Dict[str, str]) -> str:
    """
    Gets the item id for the given item name.
    :param item_name: the item name
    :param item_ids: the item ids dictionary
    :return: the item id, or the md5 hash of the item name if the item id is not found
    """
    if item_name in item_ids:
        return item_ids[item_name]

    return hashlib.md5(item_name.encode()).hexdigest()


def get_cached_data(cache: redis.Redis, url: str) -> Any:
    """
    Gets the cached data for the given URL.
    :param cache: The Redis cache, or None if no cache is available.
    :param url: The URL to get cached data for.
    :return: The cached data, or None if no cache is available or the data is not in the cache.
    """
    if cache is None:
        return None

    data = cache.get(url)
    if data is not None:
        logger.debug(f"Using cached data for {url}")
        return data

    return None


def set_cached_data(cache: redis.Redis, cache_key: str, data: Any, expiration: int = 24 * 60 * 60) -> None:
    """
    Sets the cached data for the given URL.
    :param cache: The Redis cache, or None if no cache is available.
    :param cache_key: The key to use for the cache.
    :param data: The data to cache.
    :param expiration: The expiration time for cache data in seconds. Defaults to 24 * 60 * 60 (24 hours).
    :return: None
    """
    if cache is None:
        return

    cache.set(cache_key, data, ex=expiration)


# ------------------------------
# API Request Functions

@asynccontextmanager
async def session_manager():
    """
    The aiohttp session manager.
    :return: The aiohttp session.
    """
    async with aiohttp.ClientSession() as session:
        yield session


def get_wfm_headers(platform: str = 'pc', language: str = 'en'):
    """
    Gets the headers for warframe.market API requests.
    :param platform: the platform to use for the request, defaults to pc
    :param language: the language to use for the request, defaults to en
    :return: headers dictionary for warframe.market API requests
    """
    return {'platform': platform, 'language': language}


async def fetch_api_data(session: aiohttp.ClientSession,
                         url: str,
                         headers: dict[str, str] = None,
                         cache: redis.Redis = None,
                         expiration: int = 24 * 60 * 60,
                         rate_limiter: AsyncLimiter = None,
                         return_type: str = 'json') -> Any:
    """
    Asynchronously fetch data from the given URL.

    Args:
        session (aiohttp.ClientSession): The HTTP session to use for making the request.
        url (str): The URL to fetch data from.
        headers (dict[str, str], optional): Headers to include in the request. Defaults to None.
        cache (redis.Redis | None, optional): An optional Redis instance to use for caching data. If provided,
                                               this function will attempt to fetch data from the cache before
                                               making a request. It will also store the fetched data in the cache.
                                               Defaults to None.
        expiration (int, optional): The expiration time for cache data in seconds. Defaults to 24 * 60 * 60 (24 hours).
        rate_limiter (AsyncLimiter, optional): An optional rate limiter to use. If provided, this function will
                                               acquire a token from the rate limiter before making a request.
                                               Defaults to None.
        return_type: The type to return. Defaults to JSON. Options are JSON, text, and bytes.

    Returns:
        dict: The JSON data fetched from the URL.

    Raises:
        aiohttp.ClientResponseError: If the request to the URL results in an HTTP error.
    """
    # Check if the data is in the cache, if one is provided
    if headers is None:
        headers = {}

    data = get_cached_data(cache=cache,
                           url=f"{url}#{headers}")
    if data is None:
        @retry(stop=stop_after_attempt(5), wait=wait_exponential(max=60))
        async def make_request():
            if rate_limiter is not None:
                await rate_limiter.acquire()

            async with session.get(url, headers=headers) as res:
                if res.status == 404:
                    return None

                res.raise_for_status()
                logger.debug(f"Fetched data for {url}")
                if return_type == 'json':
                    return await res.json()
                elif return_type == 'text':
                    return await res.text()
                elif return_type == 'bytes':
                    return await res.content.read()

        # Makes the API request, retrying up to 5 times if it fails, waiting 1 second between each attempt
        data = await make_request()

        # Store the data in the cache, if one is provided
        if return_type == 'json':
            cached_data = json.dumps(data)
        elif return_type == 'text':
            cached_data = str(data)
        elif return_type == 'bytes':
            cached_data = data

        set_cached_data(cache, f"{url}#{headers}", cached_data, expiration)
    else:
        if return_type == 'json':
            data = json.loads(data)

    return data


# ------------------------------
# Logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()


# ------------------------------
# Misc Functions

async def fix_names_and_add_ids(data, translation_dict, item_ids) -> None:
    """
    Fixes the item names in the given data, and adds the item ids to the data.
    :param data: the statistic history data
    :param translation_dict: the translation dictionary used to change old item names to new item names
    :param item_ids: the item ids dictionary
    :return: None
    """
    for item_name in data:
        for day in data[item_name]:
            if item_name in translation_dict:
                item_name = translation_dict[item_name]

            if 'order_type' not in day:
                day['order_type'] = 'closed'

            day["item_id"] = get_item_id(item_name, item_ids)


def get_platform_path(platform: str):
    """
    Gets the platform path for the given platform.
    :param platform: the platform to get the path for
    :return: the platform path, which is an empty string for pc, and the platform/ for other platforms
    """
    if platform == 'pc':
        return ''
    else:
        return f"{platform}/"

def get_statistic_path(platform: str):
    """
    Gets the path to save the statistic history to for the given platform.
    :param platform:
    :return:
    """
    return os.path.join(config['output_dir'], get_platform_path(platform))