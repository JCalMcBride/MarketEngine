import asyncio
import json
import os

from aiohttp import ClientResponseError
from bs4 import BeautifulSoup

from market_engine.common import fetch_api_data, logger, get_item_id, config

RELICS_RUN_BASE_URL = "https://relics.run"
RELICS_RUN_HISTORY_URL = f"{RELICS_RUN_BASE_URL}/history"


async def fix_names_and_add_ids(data, translation_dict, item_ids):
    for item_name in data:
        for day in data[item_name]:
            if item_name in translation_dict:
                item_name = translation_dict[item_name]

            day["item_id"] = get_item_id(item_name, item_ids)


async def fetch_statistics_from_relics_run(item_ids, date_list, cache, session) -> None:
    async def fetch_data(date):
        url = f"https://relics.run/history/{date}"

        try:
            data = await fetch_api_data(session=session, url=url)
        except ClientResponseError:
            logger.error(f"Failed to fetch data for {url}")
            return

        await fix_names_and_add_ids(data, translation_dict, item_ids)

        with open(os.path.join(config['output_dir'], date), 'w') as f:
            json.dump(data, f)

    translation_dict = await fetch_translation_dict_from_relics_run(cache, session)

    await asyncio.gather(*[fetch_data(date) for date in date_list])


async def get_price_history_dates(cache, session) -> set:
    data = await fetch_api_data(cache=cache,
                                session=session,
                                url=RELICS_RUN_HISTORY_URL,
                                return_json=False)

    soup = BeautifulSoup(data, 'html.parser')

    urls = set()
    for link_obj in soup.find_all('a'):
        link = link_obj.get('href')
        if link.endswith('json'):
            urls.add(link)

    return urls


def get_saved_data():
    saved_data = set()
    if not os.path.exists(config['output_dir']):
        os.makedirs(config['output_dir'])

    for file in os.listdir(config['output_dir']):
        if file.endswith(".json"):
            saved_data.add(file)

    return saved_data


async def get_dates_to_fetch(cache, session):
    date_list = await get_price_history_dates(cache, session)
    saved_data = get_saved_data()
    date_list = date_list - saved_data

    return date_list


async def fetch_item_ids_from_relics_run(cache, session):
    url = f"{RELICS_RUN_BASE_URL}/market_data/item_ids.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_item_info_from_relics_run(cache, session):
    url = f"{RELICS_RUN_BASE_URL}/market_data/item_info.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_items_from_relics_run(cache, session):
    url = f"{RELICS_RUN_BASE_URL}/market_data/items.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_translation_dict_from_relics_run(cache, session):
    url = f"{RELICS_RUN_BASE_URL}/market_data/translation_dict.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_item_data_from_relics_run(cache, session):
    tasks = [fetch_items_from_relics_run(cache, session),
             fetch_item_ids_from_relics_run(cache, session),
             fetch_item_info_from_relics_run(cache, session),
             fetch_translation_dict_from_relics_run(cache, session)]

    return await asyncio.gather(*tasks)