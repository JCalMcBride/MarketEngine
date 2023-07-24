from __future__ import annotations

import asyncio
import hashlib
import json
import lzma
import os
import traceback
import uuid
from asyncio import sleep
from collections import defaultdict
from json import JSONDecodeError
from typing import Dict, Any, List

from aiohttp import ClientResponseError
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from market_engine.common import logger, session_manager, cache_manager, fetch_api_data

API_BASE_URL = "https://api.warframe.market/v1"
ITEMS_ENDPOINT = "/items"
STATISTICS_ENDPOINT = "/items/{}/statistics"


async def fetch_set_data(url_name, cache, session):
    url = f"{API_BASE_URL}/items/{url_name}"
    return (await fetch_api_data(cache, session, url))["payload"]["item"]["items_in_set"]


async def fetch_and_save_statistics(items, item_ids, platform: str = 'pc') -> tuple[
    defaultdict[Any, defaultdict[Any, list] | defaultdict[str, list]] | defaultdict[
        str, defaultdict[Any, list] | defaultdict[str, list]], dict[Any, Any]]:
    async with cache_manager() as cache:
        async with session_manager() as session:
            with open('data/translation_dict.json', 'r') as f:
                translation_dict = json.load(f)

            price_history_dict, item_info = await process_price_history(cache, session, items, translation_dict,
                                                                        item_ids, platform)
            save_price_history(price_history_dict, platform=platform, directory=directory)
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
                                item_ids: Dict[str, str], platform: str = 'pc') -> \
        tuple[defaultdict[Any, defaultdict[Any, list] | defaultdict[str, list]] | defaultdict[
            str, defaultdict[Any, list] | defaultdict[str, list]], dict[Any, Any]]:
    price_history_dict = defaultdict(lambda: defaultdict(list))
    item_info = {}

    async def fetch_and_process_item_statistics(item, platform):
        api_data = await fetch_item_statistics(cache, session, item["url_name"], platform)
        price_history = api_data["payload"]
        logger.info(f"Processing {item['item_name']}")
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

    await asyncio.gather(*[fetch_and_process_item_statistics(item, platform) for item in items])

    return price_history_dict, item_info


async def fetch_items_from_market(cache, session) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
    return (await fetch_api_data(cache=cache,
                                 session=session,
                                 url=url))["payload"]["items"]


async def build_item_ids(items):
    item_ids = {}
    for item in items:
        item_ids[item["item_name"]] = item["id"]

    return item_ids


async def fetch_item_statistics(cache, session, item_url_name: str, platform: str = 'pc') -> Dict[str, Any]:
    url = f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item_url_name)}?include=item"

    headers = {'platform': platform, 'language': 'en'}

    return await fetch_api_data(cache, session, url, headers)


def save_price_history(price_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]],
                       directory: str = "output", platform: str = 'pc'):
    if platform == 'pc':
        platform = ''
    else:
        platform = f"/{platform}"

    for day, history in price_history_dict.items():
        filename = f"{directory}{platform}/price_history_{day}.json"
        if not os.path.isfile(filename):
            with open(filename, "w") as fp:
                json.dump(history, fp)
