from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from typing import Dict, Any, List

import aiohttp
import redis

from ...common import logger, fetch_api_data, config

API_BASE_URL = "https://api.warframe.market/v1"
ITEMS_ENDPOINT = "/items"
STATISTICS_ENDPOINT = "/items/{}/statistics"


async def fetch_items_from_warframe_market(cache, session) -> List[Dict[str, Any]]:
    url = f"{API_BASE_URL}{ITEMS_ENDPOINT}"
    return (await fetch_api_data(cache=cache,
                                 session=session,
                                 url=url))["payload"]["items"]


def build_item_ids(items):
    return {item["item_name"]: item["id"] for item in items}


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


async def fetch_statistics_from_warframe_market(cache: redis.Redis | None,
                                                session: aiohttp.ClientSession,
                                                platform: str = 'pc'):
    items = await fetch_items_from_warframe_market(cache=cache, session=session)
    item_ids = build_item_ids(items)

    statistic_history_dict = defaultdict(lambda: defaultdict(list))
    item_info = {}

    time_periods = ['90days']
    statistic_types = ['statistics_closed', 'statistics_live']

    async def fetch_and_process_item_statistics(item: Dict[str, str]):
        api_data = (await fetch_api_data(cache=cache,
                                         session=session,
                                         url=f"{API_BASE_URL}{STATISTICS_ENDPOINT.format(item['url_name'])}?include=item",
                                         headers={'platform': platform, 'language': 'en'}))

        item_name = item["item_name"]

        logger.info(f"Processing {item_name}")
        item_info[item['id']] = parse_item_info(api_data["include"]["item"])

        for statistic_type in statistic_types:
            for time_period in time_periods:
                for statistic_record in api_data['payload'][statistic_type][time_period]:
                    date = statistic_record["datetime"].split("T")[0]
                    statistic_record["item_id"] = item_ids[item_name]

                    if 'order_type' not in statistic_record:
                        statistic_record['order_type'] = 'closed'

                    statistic_history_dict[date][item_name].append(statistic_record)

    await asyncio.gather(*[fetch_and_process_item_statistics(item) for item in items[:10]])

    return statistic_history_dict, item_info


def save_statistic_history(statistic_history_dict: Dict[str, Dict[str, List[Dict[str, Any]]]],
                           platform: str = 'pc'):
    if platform == 'pc':
        platform_path = ''
    else:
        platform_path = f"{platform}"

    output_dir = os.path.join(config['output_dir'], platform_path)
    print(output_dir)
    os.makedirs(output_dir, exist_ok=True)  # Create directory if it does not exist

    for day, history in statistic_history_dict.items():
        filename = os.path.join(output_dir, f"price_history_{day}.json")

        # Handle file writing errors
        try:
            if not os.path.isfile(filename):
                with open(filename, "w") as fp:
                    json.dump(history, fp)
        except Exception as e:
            print(f"Error writing to file {filename}: {str(e)}")
