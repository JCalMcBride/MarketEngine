import configparser
import logging
from contextlib import asynccontextmanager, contextmanager

import aiohttp
import redis
from aiolimiter import AsyncLimiter


@asynccontextmanager
async def session_manager():
    async with aiohttp.ClientSession() as session:
        yield session


@asynccontextmanager
async def cache_manager():
    cache = redis.Redis(host='localhost', port=6379, db=0)
    yield cache


@contextmanager
def managed_transaction(connection):
    try:
        connection.begin()
        yield
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e


# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

OUTPUT_DIRECTORY = config.get("Output", "output_directory")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

rate_limiter = AsyncLimiter(3, 1)  # 3 requests per 1 second
