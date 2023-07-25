import configparser
import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager, contextmanager
from copy import copy
from typing import Any, Dict

import aiohttp
import redis
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_fixed

# ------------------------------
# Config

default_config = {
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_db": 0,
    "output_dir": "output"
}

config = {}

if os.path.isfile("config.json"):
    with open("config.json", "r") as f:
        config.update(json.load(f))

config.update(default_config)


# ------------------------------
# Cache Functions

@asynccontextmanager
async def cache_manager():
    cache = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])
    yield cache


def get_item_id(item_name, item_ids):
    if item_name in item_ids:
        return item_ids[item_name]

    return hashlib.md5(item_name.encode()).hexdigest()


def get_cached_data(cache: redis.Redis | None, url: str) -> Any | None:
    if cache is None:
        return None

    data = cache.get(url)
    if data is not None:
        logger.debug(f"Using cached data for {url}")
        return data

    return None


def set_cached_data(cache: redis.Redis | None, cache_key: str, data: Any, expiration: int = 24 * 60 * 60):
    if cache is None:
        return

    cache.set(cache_key, data, ex=expiration)


# ------------------------------
# API Request Functions

wfm_rate_limiter = AsyncLimiter(3, 1)  # 3 requests per 1 second


@asynccontextmanager
async def session_manager():
    async with aiohttp.ClientSession() as session:
        yield session


async def fetch_api_data(session: aiohttp.ClientSession,
                         url: str,
                         headers: dict[str, str] = None,
                         cache: redis.Redis | None = None,
                         expiration: int = 24 * 60 * 60,
                         rate_limiter: AsyncLimiter = None,
                         return_json: bool = True) -> Any:
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
        return_json: Whether to return the data as JSON or as a string.

    Returns:
        dict: The JSON data fetched from the URL.

    Raises:
        aiohttp.ClientResponseError: If the request to the URL results in an HTTP error.
    """
    # Check if the data is in the cache, if one is provided
    data = get_cached_data(cache, url)
    if data is None:
        if rate_limiter is not None:
            await rate_limiter.acquire()

        if headers is None:
            headers = {}

        @retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
        async def make_request():
            async with session.get(url, headers=headers) as res:
                res.raise_for_status()
                logger.debug(f"Fetched data for {url}")
                return await res.json() if return_json else await res.text()

        # Makes the API request, retrying up to 5 times if it fails, waiting 1 second between each attempt
        data = await make_request()

        # Store the data in the cache, if one is provided
        set_cached_data(cache, f"{url}#{headers}", str(data), expiration)

    return data


# ------------------------------
# Logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# ------------------------------
# Misc Functions

async def fix_names_and_add_ids(data, translation_dict, item_ids):
    for item_name in data:
        for day in data[item_name]:
            if item_name in translation_dict:
                item_name = translation_dict[item_name]

            if 'order_type' not in day:
                day['order_type'] = 'closed'

            day["item_id"] = get_item_id(item_name, item_ids)