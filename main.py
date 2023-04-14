import argparse
import asyncio
import configparser
import json
import logging
import os
import time
from collections import defaultdict
from typing import Dict, List, Any

import aiohttp
import diskcache as diskcache

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

API_BASE_URL = config.get('API', 'base_url')
ITEMS_ENDPOINT = config.get('API', 'items_endpoint')
STATISTICS_ENDPOINT = config.get('API', 'statistics_endpoint')
OUTPUT_DIRECTORY = config.get('Output', 'output_directory')

CACHE_EXPIRATION_SECONDS = 24 * 60 * 60  # 24 hours
cache = diskcache.Cache("api_cache", size_limit=10 ** 9)  # Adjust the size limit according to your needs

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "warframe-market-price-history-script/1.0"
}


async def fetch_api_data(session, url: str) -> Dict[str, Any]:
    if url in cache:
        timestamp, data = cache[url]
        if (time.time() - timestamp) <= CACHE_EXPIRATION_SECONDS:
            logger.debug(f"Using cached data for {url}")
            return data

    async with session.get(url, headers=HEADERS) as response:
        response.raise_for_status()
        data = await response.json()
        cache.set(url, (time.time(), data))
        logger.debug(f"Fetched and cached data for {url}")
        return data


async def fetch_all_items(session) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
    return (await fetch_api_data(session, url))['payload']['items']


async def fetch_item_statistics(session, item_url_name: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item_url_name)}"
    return (await fetch_api_data(session, url))['payload']


async def process_price_history(session, items: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    price_history_dict = defaultdict(lambda: defaultdict(list))

    for item in items:
        try:
            price_history = await fetch_item_statistics(session, item['url_name'])

            logger.info(f"Processing {item['item_name']}")

            for ph in [price_history['statistics_closed']['90days'], price_history['statistics_live']['90days']]:
                for ph_day in ph:
                    date = ph_day['datetime'].split('T')[0]
                    price_history_dict[date][item['item_name']].append(ph_day)
        except Exception as e:
            logger.error(f"Error fetching statistics for {item['item_name']}: {e}")
            continue

    return price_history_dict


def save_price_history(price_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]], output_directory: str):
    for day, history in price_history_dict.items():
        filename = f"{output_directory}/price_history_{day}.json"
        if not os.path.isfile(filename):
            with open(filename, 'w') as fp:
                json.dump(history, fp)


async def main(args):
    output_directory = args.output_directory if args.output_directory else OUTPUT_DIRECTORY

    async with aiohttp.ClientSession() as session:
        items = await fetch_all_items(session)
        price_history_dict = await process_price_history(session, items)
        save_price_history(price_history_dict, output_directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and process Warframe Market price history.")
    parser.add_argument("-o", "--output-directory", help="Output directory for price history JSON files.")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the log level.")

    args = parser.parse_args()

    if args.log_level:
        logger.setLevel(args.log_level)

    asyncio.run(main(args))
