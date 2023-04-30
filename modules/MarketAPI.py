from __future__ import annotations

import asyncio
import json
import lzma
import os
import uuid
from asyncio import sleep
from collections import defaultdict
from json import JSONDecodeError
from typing import Dict, Any, List

from aiohttp import ClientResponseError, ServerDisconnectedError
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

import common

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


def save_manifest(manifest_dict):
    for item in manifest_dict:
        with open(f"data/manifest_{item}.json", "w") as f:
            json.dump(manifest_dict, f)


async def get_manifest():
    async with common.session_manager() as session, common.cache_manager() as cache:
        wf_manifest = await fix(cache, session)
        wf_manifest = wf_manifest.split('\r\n')
        manifest_dict = {}
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

                manifest_dict[item.split("_en")[0]] = json_file
            except JSONDecodeError:
                pass
            except ClientResponseError:
                common.logger.error(f"Failed to fetch manifest {item}")

        save_manifest(manifest_dict)

        return manifest_dict


async def get_price_history_dates(cache, session) -> set:
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

    urls = set()
    for link_obj in soup.find_all('a'):
        link = link_obj.get('href')
        if link.endswith('json'):
            urls.add(link)

    return urls


def get_saved_data():
    saved_data = set()
    for file in os.listdir("output"):
        if file.endswith(".json"):
            saved_data.add(file)

    return saved_data


async def get_dates_to_fetch(cache, session):
    date_list = await get_price_history_dates(cache, session)
    saved_data = get_saved_data()
    date_list = date_list - saved_data

    return date_list


async def fetch_premade_statistics() -> None:
    async def fetch_data(date):
        url = f"https://relics.run/history/{date}"

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                common.logger.debug(f"Fetched data for {url}")
        except ClientResponseError:
            common.logger.error(f"Failed to fetch data for {date}")
            return
        except ServerDisconnectedError:
            common.logger.error(f"Failed to fetch data for {date}")
            return

        with open(os.path.join('output', date), 'w') as f:
            json.dump(data, f)

    async with common.cache_manager() as cache:
        async with common.session_manager() as session:
            date_list = await get_dates_to_fetch(cache, session)

            await asyncio.gather(*[fetch_data(date) for date in date_list])


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


def save_item_info(item_info):
    with open('data/item_info.json', 'w') as f:
        json.dump(item_info, f)


async def fetch_and_save_statistics(items, item_ids) -> tuple[
    defaultdict[Any, defaultdict[Any, list] | defaultdict[str, list]] | defaultdict[
        str, defaultdict[Any, list] | defaultdict[str, list]], list[Any]]:
    async with common.cache_manager() as cache:
        async with common.session_manager() as session:
            with open('data/translation_dict.json', 'r') as f:
                translation_dict = json.load(f)

            price_history_dict, item_info = await process_price_history(cache, session, items, translation_dict,
                                                                        item_ids)
            save_price_history(price_history_dict)
            save_item_info(item_info)

    return price_history_dict, item_info


def parse_item_info(item_info):
    parsed_info = {'set_items': [], 'item_id': item_info['id'], 'tags': [], 'mod_max_rank': None, 'subtypes': []}
    set_root = False
    for item in item_info['items_in_set']:
        if item['id'] == item_info['id']:
            if 'set_root' in item:
                set_root = item['set_root']

            parsed_info['tags'] = item['tags']
            if 'mod_max_rank' in item:
                parsed_info['mod_max_rank'] = item['mod_max_rank']

            if 'subtypes' in item:
                parsed_info['subtypes'] = item['subtypes']
        else:
            parsed_info['set_items'].append(item['id'])

    if not set_root:
        parsed_info['set_items'] = []

    return parsed_info


async def process_price_history(cache, session, items: List[Dict[str, Any]], translation_dict: Dict[str, str],
                                item_ids: Dict[str, str]) -> \
        tuple[defaultdict[Any, defaultdict[Any, list] | defaultdict[str, list]] | defaultdict[
            str, defaultdict[Any, list] | defaultdict[str, list]], dict[Any, Any]]:
    price_history_dict = defaultdict(lambda: defaultdict(list))
    item_info = {}

    async def fetch_and_process_item_statistics(item):
        api_data = await fetch_item_statistics(cache, session, item["url_name"])
        price_history = api_data["payload"]
        common.logger.info(f"Processing {item['item_name']}")
        item_info[item['id']] = parse_item_info(api_data["include"]["item"])

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

    return price_history_dict, item_info


async def fetch_all_items() -> List[Dict[str, Any]]:
    async with common.session_manager() as session, common.cache_manager() as cache:
        url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
        return (await fetch_api_data(cache, session, url))["payload"]["items"]


async def fetch_and_save_items_and_ids():
    items = await fetch_all_items()
    async with common.cache_manager() as cache:
        item_ids = get_cached_data(cache, 'data/item_ids.json')
        if item_ids is None:
            common.logger.info("Building Item IDs")
            with open('data/translation_dict.json', 'r') as f:
                translation_dict = json.load(f)

            item_ids = build_item_ids(items, translation_dict)
            cache.set('data/item_ids.json', json.dumps(item_ids), ex=60 * 60 * 24 * 7)
        else:
            common.logger.info("Loaded Item IDs from cache")
            item_ids = json.loads(item_ids)

    with open('data/item_ids.json', 'w') as f:
        json.dump(item_ids, f)

    with open('data/items.json', 'w') as f:
        json.dump(items, f)

    return items, item_ids


async def fetch_item_statistics(cache, session, item_url_name: str) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item_url_name)}?include=item"
    return await fetch_api_data(cache, session, url)


def save_price_history(price_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]],
                       directory: str = common.OUTPUT_DIRECTORY):
    for day, history in price_history_dict.items():
        filename = f"{directory}/price_history_{day}.json"
        if not os.path.isfile(filename):
            with open(filename, "w") as fp:
                json.dump(history, fp)


def fetch_premade_item_data():
    with open('data/items.json', 'r') as f:
        items = json.load(f)

    with open('data/item_ids.json', 'r') as f:
        item_ids = json.load(f)

    with open('data/item_info.json', 'r') as f:
        item_info = json.load(f)

    return items, item_ids, item_info


def fetch_premade_manifest():
    manifest_list = []
    for filename in os.listdir('data'):
        if filename.startswith("manifest"):
            with open(f"{common.OUTPUT_DIRECTORY}/{filename}", "r") as fp:
                yield json.load(fp)