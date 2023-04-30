import asyncio
import json
import lzma
import os
import uuid
from asyncio import sleep
from collections import defaultdict
from json import JSONDecodeError
from typing import Dict, Any, List

from aiohttp import ClientResponseError
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

import common
import modules.MarketDB as MarketDB

MANIFEST_URL = "https://content.warframe.com/PublicExport/index_en.txt.lzma"
API_BASE_URL = "https://api.warframe.market/v1"
ITEMS_ENDPOINT = "/items"
STATISTICS_ENDPOINT = "/items/{}/statistics"

rate_limiter = AsyncLimiter(3, 1)  # 3 requests per 1 second


def get_cached_data(cache, url: str) -> Any | None:
    data = cache.get(url)
    if data is not None:
        common.logger.debug(f"Using cached data for {url}")
        return data

    return None


async def fetch_api_data(cache, session, url: str) -> Dict[str, Any]:
    # Check if the data is in the cache
    data = get_cached_data(cache, url)
    if data is not None:
        return json.loads(data)

    await rate_limiter.acquire()  # Acquire a token from the rate limiter

    sleep_time = 0
    while True:
        # Make the API request
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                common.logger.debug(f"Fetched data for {url}")

                # Store the data in the cache with a 24-hour expiration
                cache.set(url, json.dumps(data), ex=24 * 60 * 60)

                return data
        except ClientResponseError:
            sleep_time += 1
            await sleep(sleep_time)


def decompress_lzma(data):
    results = []
    while True:
        decomp = lzma.LZMADecompressor(lzma.FORMAT_AUTO, None, None)
        try:
            res = decomp.decompress(data)
        except lzma.LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


async def fix(cache, session):
    data = get_cached_data(cache, MANIFEST_URL)
    if data is None:
        try:
            async with session.get(MANIFEST_URL) as response:
                response.raise_for_status()
                data = await response.content.read()
                common.logger.debug(f"Fetched data for {MANIFEST_URL}")

                # Store the data in the cache with a 24-hour expiration
                cache.set(MANIFEST_URL, data, ex=24 * 60 * 60)
        except ClientResponseError:
            common.logger.error("Failed to fetch manifest index")
            return

    byt = bytes(data)
    length = len(data)
    stay = True
    while stay:
        stay = False
        try:
            decompress_lzma(byt[0:length])
        except lzma.LZMAError:
            length -= 1
            stay = True

    return decompress_lzma(byt[0:length]).decode("utf-8")


async def get_manifest(cache, session):
    wf_manifest = await fix(cache, session)
    wf_manifest = wf_manifest.split('\r\n')
    manifest_list = {}
    for item in wf_manifest:
        try:
            url = f"http://content.warframe.com/PublicExport/Manifest/{item}"
            data = get_cached_data(cache, url)
            if data is None:
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.text()
                    common.logger.debug(f"Fetched data for {url}")

                    # Store the data in the cache with a 24-hour expiration
                    cache.set(url, data, ex=24 * 60 * 60)

            json_file = json.loads(data, strict=False)

            manifest_list[item.split("_en")[0]] = json_file
        except JSONDecodeError:
            pass
        except ClientResponseError:
            common.logger.error(f"Failed to fetch manifest {item}")

    return manifest_list


async def get_price_history_dates(cache, session) -> List:
    url = 'https://relics.run/history/'

    # Check if the data is in the cache
    data = get_cached_data(cache, url)
    if data is None:
        # Make the API request
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.text()
            common.logger.debug(f"Fetched data for {url}")

            # Store the data in the cache with a 24-hour expiration
            cache.set(url, data, ex=24 * 60 * 60)

    soup = BeautifulSoup(data, 'html.parser')

    urls = []
    for link_obj in soup.find_all('a'):
        link = link_obj.get('href')
        if link.endswith('json'):
            urls.append(link)

    return urls


async def fetch_premade_data(cache, session) -> None:
    date_list = await get_price_history_dates(cache, session)

    for date in date_list:
        url = f"https://relics.run/history/{date}"
        data = get_cached_data(cache, url)
        if data is None:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                common.logger.debug(f"Fetched data for {url}")

                # Store the data in the cache with a 24-hour expiration
                cache.set(url, json.dumps(data), ex=24 * 60 * 60)
        else:
            json.loads(data)

        with open(os.path.join('output', date), 'w') as f:
            json.dump(data, f)


async def fetch_set_data(url_name, cache, session):
    url = f"{API_BASE_URL}/items/{url_name}"
    return (await fetch_api_data(cache, session, url))["payload"]["item"]["items_in_set"]


def build_item_ids(items, translation_dict):
    item_ids = {}

    for item in items:
        item_ids[item['item_name']] = item['id']

    for file in os.listdir('output'):
        if not file.endswith('.json'):
            continue

        common.logger.info(f"Processing {file}")
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

    with open('data/item_ids.json', 'w') as f:
        json.dump(item_ids, f)

    return item_ids


async def fetch_and_save_sets(cache, session):
    sets = MarketDB.get_all_sets()

    for set_id, url_name in sets.items():
        set_data = await fetch_set_data(url_name, cache, session)
        MarketDB.save_set_data([(x['id'], set_id) for x in set_data if x['id'] != set_id])


async def fetch_and_save_statistics(cache, session):
    items = await fetch_all_items(cache, session)
    with open('data/translation_dict.json', 'r') as f:
        translation_dict = json.load(f)

    item_ids = build_item_ids(items, translation_dict)
    MarketDB.save_items(items, item_ids)

    price_history_dict = await process_price_history(cache, session, items, translation_dict, item_ids)
    save_price_history(price_history_dict)


async def process_price_history(cache, session, items: List[Dict[str, Any]], translation_dict: Dict[str, str],
                                item_ids: Dict[str, str]) -> \
        Dict[str, Dict[str, List[Dict[str, Any]]]]:
    price_history_dict = defaultdict(lambda: defaultdict(list))

    async def fetch_and_process_item_statistics(item):
        price_history = await fetch_item_statistics(cache, session, item["url_name"])
        common.logger.info(f"Processing {item['item_name']}")

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


async def fetch_all_items(cache, session) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
    return (await fetch_api_data(cache, session, url))["payload"]["items"]


async def fetch_item_statistics(cache, session, item_url_name: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item_url_name)}"
    return (await fetch_api_data(cache, session, url))["payload"]


def save_price_history(price_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]]):
    for day, history in price_history_dict.items():
        filename = f"{common.OUTPUT_DIRECTORY}/price_history_{day}.json"
        if not os.path.isfile(filename):
            with open(filename, "w") as fp:
                json.dump(history, fp)
