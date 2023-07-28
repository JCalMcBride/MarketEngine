import asyncio
import json
import os
from typing import Dict, List, Tuple, Any

import aiohttp
import redis
from aiohttp import ClientResponseError
from bs4 import BeautifulSoup

from market_engine.Common import fetch_api_data, logger, config, fix_names_and_add_ids, get_platform_path, \
    get_statistic_path

RELICS_RUN_BASE_URL = "https://relics.run"  # Base URL for relics.run
RELICS_RUN_HISTORY_URL = f"{RELICS_RUN_BASE_URL}/history"  # URL for fetching statistic history

def save_statistic_history(statistic_history_dict: Dict[str, Any], date: str, platform: str = 'pc') -> None:
    """
    Saves the statistic history to the output directory as specified in the config.
    :param statistic_history_dict: dictionary of item names to statistic history
    :param platform: platform to save statistic history for
    :return: None
    """
    output_dir = get_statistic_path(platform)

    os.makedirs(output_dir, exist_ok=True)  # Create directory if it does not exist

    filename = os.path.join(output_dir, date)

    with open(filename, 'w') as f:
        json.dump(statistic_history_dict, f)


async def fetch_statistics_from_relics_run(cache: redis.Redis,
                                           session: aiohttp.ClientSession,
                                           item_ids: Dict[str, str],
                                           date_list: List[str],
                                           platform: str = 'pc') -> None:
    """
    Fetches item statistics from relics.run, and saves them to the output directory as specified in the config.
    :param cache: redis cache
    :param session: aiohttp session
    :param item_ids: dictionary of item names to item ids
    :param date_list: list of dates to fetch
    :param platform: platform to fetch statistics for
    return None
    """

    async def fetch_data(date):
        url = f"https://relics.run/history/{get_platform_path(platform)}{date}"

        try:
            data = await fetch_api_data(session=session, url=url)
        except ClientResponseError:
            logger.error(f"Failed to fetch data for {url}")
            return

        await fix_names_and_add_ids(data, translation_dict, item_ids)

        save_statistic_history(data, date, platform)

    translation_dict = await fetch_translation_dict_from_relics_run(cache, session)

    await asyncio.gather(*[fetch_data(date) for date in date_list])


async def get_all_saved_dates_from_relics_run(cache: redis.Redis,
                                              session: aiohttp.ClientSession,
                                              platform: str = 'pc') -> set:
    """
    Fetches the dates of all saved statistics from relics.run
    :param platform: platform to fetch statistics for
    :param cache: redis cache
    :param session: aiohttp session
    :return: set of saved dates
    """
    data = await fetch_api_data(cache=cache,
                                session=session,
                                url=f"{RELICS_RUN_HISTORY_URL}/{get_platform_path(platform)}",
                                return_type='text')

    # Parses the HTML and finds all links to JSON files
    soup = BeautifulSoup(data, 'html.parser')

    urls = set()
    for link_obj in soup.find_all('a'):
        link = link_obj.get('href')
        if link.endswith('json'):
            urls.add(link)

    return urls


def get_saved_data(platform: str = 'pc') -> set:
    """
    Gets the names of all saved statistics from the output directory
    :param platform: platform for which to get saved statistics
    :return: set of saved statistics
    """
    saved_data = set()
    if not os.path.exists(config['output_dir']):
        os.makedirs(config['output_dir'])

    output_dir = get_statistic_path(platform)

    os.makedirs(output_dir, exist_ok=True)  # Create directory if it does not exist

    for file in os.listdir(get_statistic_path(platform)):
        if file.endswith(".json"):
            saved_data.add(file)

    return saved_data


async def get_dates_to_fetch(cache: redis.Redis,
                             session: aiohttp.ClientSession,
                             platform: str = 'pc') -> set:
    """
    Gets the dates for which statistics need to be fetched
    :param cache: redis cache
    :param session: aiohttp session
    :param platform: platform for which to get dates
    :return: set of dates to fetch
    """
    date_list = await get_all_saved_dates_from_relics_run(cache, session, platform)  # Get all possible dates
    print(date_list)
    saved_data = get_saved_data(platform)  # Get the dates for which statistics have already been fetched
    date_list = date_list - saved_data  # Remove the dates for which statistics have already been fetched

    return date_list


async def fetch_item_ids_from_relics_run(cache: redis.Redis,
                                         session: aiohttp.ClientSession) -> Dict[str, str]:
    """
    Fetches item ids from relics.run
    :param cache: redis cache
    :param session: aiohttp session
    :return: dictionary of item names to item ids
    """
    url = f"{RELICS_RUN_BASE_URL}/market_data/item_ids.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_item_info_from_relics_run(cache: redis.Redis,
                                          session: aiohttp.ClientSession) -> Dict[str, str]:
    """
    Fetches item info from relics.run
    :param cache: redis cache
    :param session: aiohttp session
    :return: dictionary of item names to item info
    """
    url = f"{RELICS_RUN_BASE_URL}/market_data/item_info.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_items_from_relics_run(cache: redis.Redis,
                                      session: aiohttp.ClientSession) -> List[Dict[str, str]]:
    """
    Fetches list of warframe.market items from relics.run
    :param cache: redis cache
    :param session: aiohttp session
    :return: list of warframe.market items
    """
    url = f"{RELICS_RUN_BASE_URL}/market_data/items.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_translation_dict_from_relics_run(cache: redis.Redis,
                                                 session: aiohttp.ClientSession) -> Dict[str, str]:
    """
    Fetches translation dictionary from relics.run
    :param cache: redis cache
    :param session: aiohttp session
    :return: Dictionary translating old/changed item names to corrected versions
    """
    url = f"{RELICS_RUN_BASE_URL}/market_data/translation_dict.json"
    return await fetch_api_data(cache=cache, session=session, url=url)


async def fetch_item_data_from_relics_run(cache: redis.Redis,
                                          session: aiohttp.ClientSession) -> list[Any]:
    """
    Fetches all item data from relics.run
    :param cache: redis cache
    :param session: aiohttp session
    :return: list of items, item_ids, item_info, and translation_dict
    """
    tasks = [fetch_items_from_relics_run(cache, session),
             fetch_item_ids_from_relics_run(cache, session),
             fetch_item_info_from_relics_run(cache, session),
             fetch_translation_dict_from_relics_run(cache, session)]

    return await asyncio.gather(*tasks)
