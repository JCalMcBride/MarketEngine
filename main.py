import argparse
import asyncio
import configparser
import json
import logging
import os
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import Dict, List, Any

import aiohttp
import pymysql
import redis
from aiolimiter import AsyncLimiter
from pytz import timezone

import categories

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

API_BASE_URL = config.get("API", "base_url")
ITEMS_ENDPOINT = config.get("API", "items_endpoint")
STATISTICS_ENDPOINT = config.get("API", "statistics_endpoint")
OUTPUT_DIRECTORY = config.get("Output", "output_directory")

CACHE_EXPIRATION_SECONDS = 24 * 60 * 60  # 24 hours

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

HEADERS = {
    "User-Agent": "warframe-market-price-history-script/1.0"
}

rate_limiter = AsyncLimiter(3, 1)  # 3 requests per 1 second


@asynccontextmanager
async def session_manager():
    async with aiohttp.ClientSession() as session:
        yield session


@asynccontextmanager
async def cache_manager():
    cache = redis.Redis(host='localhost', port=6379, db=0)
    yield cache


async def fetch_api_data(session, cache, url: str) -> Dict[str, Any]:
    # Check if the data is in the cache
    data = cache.get(url)
    if data is not None:
        logger.debug(f"Using cached data for {url}")
        return json.loads(data)

    await rate_limiter.acquire()  # Acquire a token from the rate limiter

    # Make the API request
    async with session.get(url, headers=HEADERS) as response:
        response.raise_for_status()
        data = await response.json()
        logger.debug(f"Fetched data for {url}")

        # Store the data in the cache with a 24-hour expiration
        cache.set(url, json.dumps(data), ex=24 * 60 * 60)

        return data


async def fetch_all_items(session, cache) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
    return (await fetch_api_data(session, cache, url))["payload"]["items"]


async def fetch_item_statistics(session, cache, item_url_name: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item_url_name)}"
    return (await fetch_api_data(session, cache, url))["payload"]


async def process_price_history(session, cache, items: List[Dict[str, Any]], translation_dict: Dict[str, str],
                                item_ids: Dict[str, str]) -> \
        Dict[str, Dict[str, List[Dict[str, Any]]]]:
    price_history_dict = defaultdict(lambda: defaultdict(list))

    async def fetch_and_process_item_statistics(item):
        price_history = await fetch_item_statistics(session, cache, item["url_name"])
        logger.info(f"Processing {item['item_name']}")

        for ph in [price_history["statistics_closed"]["90days"], price_history["statistics_live"]["90days"]]:
            for price_history_day in ph:
                date = price_history_day["datetime"].split("T")[0]
                item_name = item["item_name"]
                if item_name in translation_dict:
                    item_name = translation_dict[item_name]

                price_history_day["item_id"] = item_ids[item_name]

                if 'order_type' not in price_history_day:
                    price_history_day['order_type'] = 'Closed'

                price_history_dict[date][item_name].append(price_history_day)

    await asyncio.gather(*[fetch_and_process_item_statistics(item) for item in items])

    return price_history_dict


def save_price_history(price_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]], output_directory: str):
    for day, history in price_history_dict.items():
        filename = f"{output_directory}/price_history_{day}.json"
        if not os.path.isfile(filename):
            with open(filename, "w") as fp:
                json.dump(history, fp)


def open_price_history_file(filename: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    with open(filename, "r") as fp:
        return json.load(fp)


@contextmanager
def managed_transaction(connection):
    try:
        connection.begin()
        yield
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e


def insert_item_statistics(data_list, connection):
    # Get the union of all keys in the data_list
    all_columns = set().union(*(data.keys() for data in data_list))
    columns_str = ', '.join(all_columns)
    placeholders = ', '.join(['%s'] * len(all_columns))

    insert_query = f"""
        INSERT IGNORE INTO item_statistics ({columns_str})
        VALUES ({placeholders})
    """

    # Create a list of values for each dictionary, using None for missing keys
    for data in data_list:
        if 'order_type' not in data:
            data['order_type'] = 'Closed'

    values = [tuple(data.get(key, None) for key in all_columns) for data in data_list]

    batch_size = 10_000
    total_batches = (len(values) + batch_size - 1) // batch_size

    with connection.cursor() as cursor:
        with managed_transaction(connection):
            for i in range(0, len(values), batch_size):
                batch_values = values[i:i + batch_size]
                cursor.executemany(insert_query, batch_values)
                print(f"Progress: Batch {i // batch_size + 1} of {total_batches} completed")


def parse_price_history(price_history):
    data_list = []
    for item in price_history:
        for statistic_type in price_history[item]:
            data_list.append(statistic_type)

    return data_list


def get_date(connection):
    query = "SELECT MAX(datetime) FROM item_statistics"

    with connection.cursor() as cursor:
        cursor.execute(query)
        most_recent_datetime = cursor.fetchone()[0]

    if most_recent_datetime is not None:
        # Convert the datetime to UTC
        utc_datetime = most_recent_datetime.astimezone(timezone('UTC'))
        most_recent_date = utc_datetime.date()
    else:
        most_recent_date = None

    return most_recent_date


def get_file_list(date, output_directory):
    file_list = []
    for file in os.listdir(output_directory):
        if file.endswith(".json"):
            if date is not None:
                file_date = datetime.strptime(file, "price_history_%Y-%m-%d.json").date()
                if file_date <= date:
                    continue

            file_list.append(os.path.join(output_directory, file))

    return file_list


def get_data_list(file_list):
    data_list = []

    for file in file_list:
        data_list.extend(parse_price_history(open_price_history_file(file)))

    return data_list


def save_items(items, item_ids, connection):
    data_list = [(item['id'], item['item_name'], item['url_name'], item['thumb']) for item in items]

    new_item_ids = {}
    for item, item_id in item_ids.items():
        if item_id not in [x['id'] for x in items]:
            new_item_ids[item] = item_id
    data_list.extend([(new_item_ids[item], item, None, None) for item in new_item_ids])

    insert_query = """
        INSERT IGNORE INTO items (id, item_name, url_name, thumb)
        VALUES (%s, %s, %s, %s)
    """

    with connection.cursor() as cursor:
        cursor.executemany(insert_query, data_list)

    connection.commit()


def get_sub_type_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT i.item_name, GROUP_CONCAT(DISTINCT s.sub_type) as subtypes
                          FROM item_subtypes s
                          JOIN items i ON s.item_id = i.id
                          GROUP BY i.item_name""")
        sub_type_data = cursor.fetchall()

        return {row[0]: row[1].split(',') for row in sub_type_data}


def get_mod_rank_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT i.item_name, GROUP_CONCAT(DISTINCT s.mod_rank) as modrank
                          FROM item_mod_ranks s
                          JOIN items i ON s.item_id = i.id
                          GROUP BY i.item_name""")
        sub_type_data = cursor.fetchall()

        return {row[0]: row[1].split(',') for row in sub_type_data}


def get_item_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT item_name, id FROM items""")

    return dict(cursor.fetchall())


def build_category_info(connection):
    manifest_list = categories.get_manifest()
    wf_parser = categories.build_parser(manifest_list)
    item_categories = categories.get_wfm_item_categorized(get_item_data(connection), manifest_list, wf_parser)

    query = """UPDATE items SET item_type = %s WHERE id = %s"""

    with connection.cursor() as cursor:
        for item_type in item_categories:
            cursor.executemany(query, [(item_type, item_id) for item_id in item_categories[item_type].values()])


def save_sub_type_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""INSERT IGNORE INTO item_subtypes (item_id, sub_type)
                          SELECT DISTINCT item_id, subtype
                          FROM item_statistics
                          WHERE subtype IS NOT NULL""")


def save_mod_type_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""INSERT IGNORE INTO item_mod_ranks (item_id, mod_rank)
                          SELECT DISTINCT item_id, mod_rank
                          FROM item_statistics
                          WHERE mod_rank IS NOT NULL""")


def build_item_ids(items, translation_dict):
    item_ids = {}

    for item in items:
        item_ids[item['item_name']] = item['id']

    for file in os.listdir('output'):
        logger.info(f"Processing {file}")
        history_file = None
        with open(os.path.join('output', file), 'r') as f:
            history_file = json.load(f)

        for item in history_file:
            for order_type in history_file[item]:
                if item not in translation_dict:
                    fixed_item = item
                else:
                    fixed_item = translation_dict[item]

                if 'subtype' not in order_type:
                    if fixed_item not in item_ids:
                        item_ids[fixed_item] = uuid.uuid4().hex[:24]

                order_type['item_id'] = item_ids[fixed_item]

        with open(os.path.join('output', file), 'w') as f:
            json.dump(history_file, f)

    with open('item_ids.json', 'w') as f:
        json.dump(item_ids, f)

    return item_ids


async def main(args):
    output_directory = args.output_directory if args.output_directory else OUTPUT_DIRECTORY

    cnx = pymysql.connect(user='market', password='Zg2UXkJ3hVkHPANGJGYu', host='localhost', database='market')

    if args.fetch:
        async with cache_manager() as cache:
            async with session_manager() as session:
                items = await fetch_all_items(session, cache)
                with open('translation_dict.json', 'r') as f:
                    translation_dict = json.load(f)

                item_ids = build_item_ids(items, translation_dict)
                save_items(items, item_ids, cnx)

                price_history_dict = await process_price_history(session, cache, items, translation_dict, item_ids)
                save_price_history(price_history_dict, output_directory)

    date = None
    if not args.database:
        date = get_date(cnx)

    file_list = get_file_list(date, output_directory)

    data_list = get_data_list(file_list)

    insert_item_statistics(data_list, cnx)

    if not args.database:
        build_category_info(cnx)
        save_sub_type_data(cnx)
        save_mod_type_data(cnx)

    cnx.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and process Warframe Market price history.")
    parser.add_argument("-o", "--output-directory", help="Output directory for price history JSON files.")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the log level.")
    parser.add_argument("-f", "--fetch", action="store_true", help="Fetch and process price history from the API.")
    parser.add_argument("-d", "--database", action="store_true", help="Save all price history to the database.")

    args = parser.parse_args()

    if args.log_level:
        logger.setLevel(args.log_level)

    asyncio.run(main(args))
