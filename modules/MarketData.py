from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from functools import wraps
from typing import List, Tuple, Optional, Dict, Any, Union

import aiohttp
import pymysql
from fuzzywuzzy import fuzz
from pymysql import Connection

from common import cache_manager, logger, session_manager, rate_limiter


async def fetch_wfm_data(url: str):
    async with cache_manager() as cache:
        data = cache.get(url)
        if data is not None:
            logger.debug(f"Using cached data for {url}")
            return json.loads(data)

    retries = 3
    async with session_manager() as session:
        for _ in range(retries):
            try:
                async with rate_limiter:
                    async with session.get(url) as r:
                        if r.status == 200:
                            logger.info(f"Fetched data from {url}")
                            data = await r.json()

                            # Store the data in the cache with a 1-minute expiration
                            cache.set(url, json.dumps(data), ex=60)

                            return await r.json()
                        else:
                            raise aiohttp.ClientError
            except aiohttp.ClientError:
                logger.error(f"Failed to fetch data from {url}")


def get_item_names(item: Dict[str, Any]) -> List[str]:
    return [item['item_name']] + item.get('aliases', [])


def closest_common_word(word: str, common_words: set, threshold: int) -> Optional[str]:
    best_match, best_score = None, 0
    for common_word in common_words:
        score = fuzz.ratio(word, common_word)
        if score > best_score:
            best_match, best_score = common_word, score

    return best_match if best_score >= threshold else None


def remove_common_words(name: str, common_words: set) -> str:
    name = remove_blueprint(name)
    threshold = 80  # Adjust this value based on the desired level of fuzzy matching

    words = name.split()
    filtered_words = [word for word in words if not closest_common_word(word, common_words, threshold)]
    return ' '.join(filtered_words)


def remove_blueprint(s: str) -> str:
    words = s.lower().split()
    if words[-1:] == ['blueprint'] and words[-2:-1] != ['prime']:
        return ' '.join(words[:-1])
    return s.lower()


def find_best_match(item_name: str, items: List[Dict[str, Any]]) -> Tuple[int, Optional[Dict[str, str]]]:
    best_score, best_item = 0, None
    common_words = {'arcane', 'prime', 'scene', 'set'}

    item_name = remove_common_words(item_name, common_words)

    for item in items:
        processed_names = [remove_common_words(name, common_words) for name in get_item_names(item)]
        max_score = max(fuzz.ratio(item_name, name) for name in processed_names)
        if max_score > best_score:
            best_score, best_item = max_score, item

        if best_score == 100:
            break

    return best_score, best_item


class MarketDatabase:
    GET_ITEM_QUERY: str = "SELECT * FROM items WHERE item_name=%s"
    GET_ITEM_SUBTYPES_QUERY: str = "SELECT * FROM item_subtypes WHERE item_id=%s"
    GET_ITEM_MOD_RANKS_QUERY: str = "SELECT * FROM item_mod_ranks WHERE item_id=%s"
    GET_ITEM_STATISTICS_QUERY: str = ("SELECT datetime, avg_price "
                                      "FROM item_statistics "
                                      "WHERE item_id=%s "
                                      "AND order_type='closed'")
    GET_ITEM_VOLUME_QUERY: str = ("SELECT volume "
                                  "FROM item_statistics "
                                  "WHERE datetime >= NOW() - INTERVAL %s DAY "
                                  "AND order_type='closed' "
                                  "AND item_id = %s")
    GET_ALL_ITEMS_QUERY: str = ("SELECT items.id, items.item_name, items.item_type, "
                                "items.url_name, items.thumb, item_aliases.alias "
                                "FROM items LEFT JOIN item_aliases ON items.id = item_aliases.item_id")
    GET_ITEMS_IN_SET_QUERY: str = ("SELECT items.id, items.item_name, items.item_type, items.url_name, items.thumb "
                                   "FROM items_in_set "
                                   "INNER JOIN items "
                                   "ON items_in_set.item_id = items.id "
                                   "WHERE items_in_set.set_id = %s")

    def __init__(self, user: str, password: str, host: str, database: str) -> None:
        self.connection: Connection = pymysql.connect(user=user,
                                                      password=password,
                                                      host=host,
                                                      database=database)

        self.all_items = self.get_all_items()

    def get_all_items(self) -> list[list]:
        all_data = self._execute_query(self.GET_ALL_ITEMS_QUERY)

        item_dict = defaultdict(lambda: defaultdict(list))
        for item_id, item_name, item_type, url_name, thumb, alias in all_data:
            if not item_dict[item_id]['item_data']:
                item_dict[item_id]['item_data'] = {'id': item_id, 'item_name': item_name, 'item_type': item_type,
                                                   'url_name': url_name, 'thumb': thumb}
            if alias:
                item_dict[item_id]['aliases'].append(alias)

        all_items = [item_data['item_data'] for item_data in item_dict.values()]

        return all_items

    def _execute_query(self, query: str, *params) -> tuple[tuple[Any, ...], ...]:
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_item(self, item: str) -> Optional[MarketItem]:
        fuzzy_item = self.get_fuzzy_item(item)

        if fuzzy_item is None:
            return None

        item_data: list[str] = list(fuzzy_item.values())

        return MarketItem(self, *item_data)

    def get_item_statistics(self, item_id: str) -> tuple[tuple[Any, ...], ...]:
        return self._execute_query(self.GET_ITEM_STATISTICS_QUERY, item_id)

    def get_item_volume(self, item_id: str, days: int = 31) -> tuple[tuple[Any, ...], ...]:
        return self._execute_query(self.GET_ITEM_VOLUME_QUERY, days, item_id)

    def close(self) -> None:
        self.connection.close()

    def get_fuzzy_item(self, item_name: str) -> Optional[Dict[str, str]]:
        best_score, best_item = find_best_match(item_name, self.all_items)

        return best_item if best_score > 50 else None

    def get_item_parts(self, item_id: str) -> tuple[tuple[Any, ...], ...]:
        return self._execute_query(self.GET_ITEMS_IN_SET_QUERY, item_id)


def require_orders():
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            await self.get_orders()
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


def require_all_part_orders():
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            tasks = [item.get_orders() for item in self.parts]
            await asyncio.gather(*tasks)
            return await func(self, *args, **kwargs)

        return wrapper

    return decorator


class MarketItem:
    base_api_url: str = "https://api.warframe.market/v1"
    base_url: str = "https://warframe.market/items"
    asset_url: str = "https://warframe.market/static/assets"

    def __init__(self, database: MarketDatabase,
                 item_id: str, item_name: str, item_type: str, item_url_name: str, thumb: str) -> None:
        self.database: MarketDatabase = database
        self.item_id: str = item_id
        self.item_name: str = item_name
        self.item_type: str = item_type
        self.item_url_name: str = item_url_name
        self.thumb: str = thumb
        self.thumb_url: str = f"{MarketItem.asset_url}/{self.thumb}"
        self.item_url: str = f"{MarketItem.base_url}/{self.item_url_name}"
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        self.parts: List[MarketItem] = []

    def filter_orders(self, order_type: str = 'sell', num_orders: int = 5, only_online: bool = True,
                      subtype: str = None) \
            -> List[Dict[str, Union[str, int]]]:
        orders = self.orders[order_type]

        if only_online:
            orders = list(filter(lambda x: x['state'] == 'ingame', orders))

        if subtype is not None:
            orders = list(filter(lambda x: x['subtype'] == subtype, orders))

        return orders[:num_orders]

    def parse_orders(self, orders: List[Dict[str, Any]]) -> None:
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        for order in orders:
            order_type = order['order_type']
            user = order['user']
            parsed_order = {
                'last_update': order['last_update'],
                'quantity': order['quantity'],
                'price': order['platinum'],
                'user': user['ingame_name'],
                'state': user['status']
            }

            if 'subtype' in order:
                parsed_order['subtype'] = order['subtype']

            if 'mod_rank' in order:
                parsed_order['subtype'] = f"R{order['mod_rank']}"

            self.orders[order_type].append(parsed_order)

        for key, reverse in [('sell', False), ('buy', True)]:
            self.orders[key].sort(key=lambda x: (x['price'], x['last_update']), reverse=reverse)

    async def get_orders(self) -> None:
        orders = await fetch_wfm_data(f"{self.base_api_url}/items/{self.item_url_name}/orders")
        if orders is None:
            return

        self.parse_orders(orders['payload']['orders'])
