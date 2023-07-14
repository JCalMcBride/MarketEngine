from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from functools import wraps
from typing import List, Tuple, Optional, Dict, Any, Union, Coroutine
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import aiohttp
import pymysql
from fuzzywuzzy import fuzz
from pymysql import Connection, InterfaceError
from pymysql.cursors import Cursor

from ..common import cache_manager, logger, session_manager, rate_limiter


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

                            # Store the data in the cache with a 15-second expiration
                            cache.set(url, json.dumps(data), ex=15)

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

    ignore_list = ['primed']

    words = name.split()
    filtered_words = [word for word in words if not (closest_common_word(word, common_words, threshold)
                                                     and word not in ignore_list)]
    return ' '.join(filtered_words)


def remove_blueprint(s: str) -> str:
    words = s.lower().split()
    if words[-1:] == ['blueprint'] and words[-2] not in ['prime', 'wraith', 'vandal']:
        return ' '.join(words[:-1])
    return s.lower()


def replace_aliases(name: str, aliases: dict, threshold=80) -> str:
    words = name.split()
    new_words = []

    for word in words:
        for alias, replacement in aliases.items():
            if fuzz.ratio(word, alias) >= threshold:
                new_words.append(replacement)
                break
        else:  # This is executed if the loop didn't break, meaning no replacement was found
            new_words.append(word)

    return ' '.join(new_words)


def find_best_match(item_name: str, items: List[Dict[str, Any]], word_aliases: List) -> Tuple[
    int, Optional[Dict[str, str]]]:
    best_score, best_item = 0, None
    common_words = {'arcane', 'prime', 'scene', 'set'}

    item_name = replace_aliases(item_name, word_aliases)
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
    _GET_ITEM_QUERY: str = "SELECT * FROM items WHERE item_name=%s"
    _GET_ITEM_SUBTYPES_QUERY: str = "SELECT * FROM item_subtypes WHERE item_id=%s"
    _GET_ITEM_STATISTICS_QUERY: str = ("SELECT datetime, avg_price "
                                       "FROM item_statistics "
                                       "WHERE item_id=%s "
                                       "AND order_type='closed'")
    _GET_ITEM_VOLUME_QUERY: str = ("SELECT volume "
                                   "FROM item_statistics "
                                   "WHERE datetime >= NOW() - INTERVAL %s DAY "
                                   "AND order_type='closed' "
                                   "AND item_id = %s")
    _BASE_ITEMS_QUERY: str = ("SELECT items.id, items.item_name, items.item_type, "
                              "items.url_name, items.thumb, items.max_rank, GROUP_CONCAT(item_aliases.alias) AS aliases")

    _GET_ALL_ITEMS_QUERY: str = (f"{_BASE_ITEMS_QUERY} "
                                 "FROM items LEFT JOIN item_aliases ON items.id = item_aliases.item_id "
                                 "GROUP BY items.id")

    _GET_ITEMS_IN_SET_QUERY: str = (f"{_BASE_ITEMS_QUERY} "
                                    "FROM items_in_set "
                                    "INNER JOIN items "
                                    "ON items_in_set.item_id = items.id "
                                    "LEFT JOIN item_aliases ON items.id = item_aliases.item_id "
                                    "WHERE items_in_set.set_id = %s "
                                    "GROUP BY items.id, items.item_name, items.item_type, items.url_name, items.thumb, items.max_rank")

    _GET_ALL_WORD_ALIASES_QUERY: str = "SELECT alias, word FROM word_aliases"

    _ADD_ITEM_ALIAS_QUERY: str = "INSERT INTO item_aliases (item_id, alias) VALUES (%s, %s)"
    _REMOVE_ITEM_ALIAS_QUERY: str = "DELETE FROM item_aliases WHERE item_id=%s AND alias=%s"

    _ADD_WORD_ALIAS_QUERY: str = "INSERT INTO word_aliases (word, alias) VALUES (%s, %s)"

    _GET_USER_QUERY = "SELECT ingame_name FROM market_users WHERE user_id=%s"
    _UPSERT_USER_QUERY = """
        INSERT INTO market_users (user_id, ingame_name) 
        VALUES (%s, %s) 
        ON DUPLICATE KEY UPDATE ingame_name=VALUES(ingame_name)
    """
    _INSERT_USERNAME_HISTORY_QUERY = """
        INSERT INTO username_history (user_id, ingame_name, datetime) 
        VALUES (%s, %s, %s)
    """
    _GET_CORRECT_CASE_QUERY = """
        SELECT user_id, ingame_name
        FROM market_users 
        WHERE LOWER(ingame_name) = LOWER(%s)
    """

    def __init__(self, user: str, password: str, host: str, database: str) -> None:
        self.connection: Connection = pymysql.connect(user=user,
                                                      password=password,
                                                      host=host,
                                                      database=database)

        self.all_items = self._get_all_items()

    def _get_all_items(self) -> List[dict]:
        all_data = self._execute_query(self._GET_ALL_ITEMS_QUERY)

        all_items: List[dict] = []
        for item_id, item_name, item_type, url_name, thumb, max_rank, alias in all_data:
            aliases = []
            if alias:
                aliases = alias.split(',')

            all_items.append({'id': item_id, 'item_name': item_name, 'item_type': item_type,
                              'url_name': url_name, 'thumb': thumb, 'max_rank': max_rank, 'aliases': aliases})

        return all_items

    def _execute_query(self, query: str, *params) -> Cursor:
        self.connection.ping(reconnect=True)

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor

    async def get_user(self, user: str) -> Optional[MarketUser]:
        cursor = self._execute_query(self._GET_CORRECT_CASE_QUERY, user)
        result = cursor.fetchone()

        if result is None:
            # Username not found, return None
            return None
        else:
            # Return the correct casing
            return await MarketUser.create(self, result[0], result[1])

    async def get_item(self, item: str, fetch_orders: bool = True,
                       fetch_parts: bool = True, fetch_part_orders: bool = True) -> Optional[MarketItem]:
        fuzzy_item = self._get_fuzzy_item(item)

        if fuzzy_item is None:
            return None

        item_data: List[str] = list(fuzzy_item.values())

        return await MarketItem.create(self, *item_data,
                                       fetch_parts=fetch_parts,
                                       fetch_orders=fetch_orders,
                                       fetch_part_orders=fetch_part_orders)

    def get_item_statistics(self, item_id: str) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEM_STATISTICS_QUERY, item_id).fetchall()

    def get_item_volume(self, item_id: str, days: int = 31) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEM_VOLUME_QUERY, days, item_id).fetchall()

    def close(self) -> None:
        self.connection.close()

    def _get_fuzzy_item(self, item_name: str) -> Optional[Dict[str, str]]:
        best_score, best_item = find_best_match(item_name, self.all_items, self.get_word_aliases())

        return best_item if best_score > 50 else None

    def get_item_parts(self, item_id: str) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEMS_IN_SET_QUERY, item_id).fetchall()

    def get_word_aliases(self) -> Dict[str, str]:
        return dict(self._execute_query(self._GET_ALL_WORD_ALIASES_QUERY).fetchall())

    def add_item_alias(self, item_id, alias):
        return self._execute_query(self._ADD_ITEM_ALIAS_QUERY, item_id, alias)

    def remove_item_alias(self, item_id, alias):
        return self._execute_query(self._REMOVE_ITEM_ALIAS_QUERY, item_id, alias)

    def add_word_alias(self, word, alias):
        return self._execute_query(self._ADD_WORD_ALIAS_QUERY, word, alias)

    def update_usernames(self, data: dict) -> None:
        with ThreadPoolExecutor() as executor:
            executor.submit(self._background_update_usernames, data)

    def _background_update_usernames(self, data: dict) -> None:
        for user_id, new_ingame_name in data.items():
            result = self._execute_query(self._GET_USER_QUERY, user_id).fetchone()

            # If the user doesn't exist or username is different,
            # update the user's username in `market_users` and add a record in `username_history`
            if result is None or new_ingame_name != result[0]:
                logger.info(f"Updating username for user {user_id} to {new_ingame_name}")
                self._execute_query(self._UPSERT_USER_QUERY, user_id, new_ingame_name)

                now = datetime.now()
                self._execute_query(self._INSERT_USERNAME_HISTORY_QUERY, user_id, new_ingame_name, now)

            # Commit your changes
            self.connection.commit()


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
                 item_id: str, item_name: str, item_type: str, item_url_name: str, thumb: str, max_rank: str,
                 aliases: List, fetch_orders: bool = True, fetch_parts: bool = True, fetch_part_orders: bool = True) -> None:
        self.database: MarketDatabase = database
        self.item_id: str = item_id
        self.item_name: str = item_name
        self.item_type: str = item_type
        self.item_url_name: str = item_url_name
        self.thumb: str = thumb
        self.max_rank: str = max_rank
        self.aliases: List = aliases
        self.thumb_url: str = f"{MarketItem.asset_url}/{self.thumb}"
        self.item_url: str = f"{MarketItem.base_url}/{self.item_url_name}"
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        self.parts: List[MarketItem] = []
        self.part_orders_fetched: bool = False

    @classmethod
    async def create(cls, database: MarketDatabase, item_id: str, item_name: str, item_type: str,
                     item_url_name: str, thumb: str, max_rank: str, aliases: List, fetch_orders: bool = True,
                     fetch_parts: bool = True, fetch_part_orders: bool = True):
        obj = cls(database, item_id, item_name, item_type, item_url_name, thumb, max_rank, aliases)

        tasks = []
        if fetch_orders:
            tasks.append(obj.get_orders())

        if fetch_parts:
            obj.get_parts()

        if fetch_part_orders:
            tasks += obj.get_part_orders_tasks()

        await asyncio.gather(*tasks)

        return obj

    @staticmethod
    def create_filters(**kwargs) -> Tuple[Dict[str, Union[int, str, List[int], List[str]]], Dict[str, str]]:
        filters = {}
        mode = {}

        for key, value in kwargs.items():
            if key.endswith('_mode'):
                field = key[:-5]
                mode[field] = value
            else:
                filters[key] = value

        return filters, mode

    def get_part_orders_tasks(self) -> list[Coroutine[Any, Any, None]]:
        tasks = [part.get_orders() for part in self.parts if part is not None]
        self.part_orders_fetched = True

        return tasks

    def add_alias(self, alias: str) -> None:
        self.database.add_item_alias(self.item_id, alias)

    def remove_alias(self, alias: str) -> None:
        self.database.remove_item_alias(self.item_id, alias)

    def filter_orders(self,
                      order_type: str = 'sell',
                      num_orders: int = 5,
                      filters: Optional[Dict[str, Union[int, str, List[int], List[str]]]] = None,
                      mode: Optional[Dict[str, str]] = None) \
            -> List[Dict[str, Union[str, int]]]:
        """
        Filters the orders based on the provided filters and mode dictionaries.

        :param order_type: The type of orders to filter ('sell' or 'buy')
        :param num_orders: The maximum number of orders to return after filtering
        :param filters: A dictionary containing the fields to filter and their corresponding filter values.
                        The keys are the field names and the values can be a string, a list of strings,
                        an integer, or a list of integers.
        :param mode: A dictionary containing the filtering mode for specific fields. The keys are the field names
                     and the values are the modes ('whitelist', 'blacklist', 'greater', 'less', or 'equal').
                     If not specified, the default mode for string-based fields is 'whitelist', while for
                     integer-based fields, it is 'equal'.
        :return: A list of filtered orders
        """
        if filters is None:
            filters = {}

        if mode is None:
            mode = {}

        def ensure_list(value):
            return [value]

        def apply_filter(value: Union[str, int], filter_value: Union[str, List, int], field: str):
            if isinstance(filter_value, str):
                filter_value = ensure_list(filter_value)

            if filter_value is None:
                return True
            filter_mode = mode.get(field)

            if isinstance(value, int):
                if filter_mode == 'greater':
                    return value > filter_value
                elif filter_mode == 'less':
                    return value < filter_value
                else:
                    return value == filter_value
            else:
                if filter_mode == 'blacklist':
                    return value not in filter_value
                else:
                    return value in filter_value

        orders = self.orders[order_type]

        filtered_orders = [order for order in orders if all(
            apply_filter(order.get(key), filter_value, key) for key, filter_value in filters.items())]

        return filtered_orders[:num_orders]

    def parse_orders(self, orders: List[Dict[str, Any]]) -> None:
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        users = {}

        for order in orders:
            order_type = order['order_type']
            user = order['user']
            users[user['id']] = user['ingame_name']

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

        self.database.update_usernames(users)

    async def get_orders(self) -> None:
        orders = await fetch_wfm_data(f"{self.base_api_url}/items/{self.item_url_name}/orders")
        if orders is None:
            return

        self.parse_orders(orders['payload']['orders'])

    def get_parts(self) -> None:
        self.parts = [MarketItem(self.database, *item) for item in self.database.get_item_parts(self.item_id)]


class MarketUser:
    base_api_url: str = "https://api.warframe.market/v1"
    base_url: str = "https://warframe.market/profile"
    asset_url: str = "https://warframe.market/static/assets"

    def __init__(self, database: MarketDatabase, user_id: int, username: str):
        self.database = database
        self.user_id = user_id
        self.username = username
        self.profile_url: str = f"{MarketUser.base_url}/{self.username}"
        self.last_seen = None
        self.avatar = None
        self.avatar_url = None
        self.locale = None
        self.background = None
        self.about = None
        self.reputation = None
        self.platform = None
        self.banned = None
        self.status = None
        self.region = None
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        self.reviews: Dict[str, List[Dict[str, Union[str, int]]]] = []

    @classmethod
    async def create(cls, database: MarketDatabase, user_id: str, username: str,
                     fetch_user_data: bool = True, fetch_orders: bool = True, fetch_reviews: bool = True):
        obj = cls(database, user_id, username)

        if fetch_user_data:
            await obj.fetch_user_data()

        if fetch_orders:
            await obj.fetch_orders()

        if fetch_reviews:
            await obj.fetch_reviews()

        return obj

    async def fetch_user_data(self) -> None:
        user_data = await fetch_wfm_data(f"{self.base_api_url}/profile/{self.username}")
        if user_data is None:
            return

        # Load the user profile
        profile = user_data['payload']['profile']

        for key, value in profile.items():
            if hasattr(self, key):
                setattr(self, key, value)

        if self.avatar is not None:
            self.avatar_url = f"{MarketUser.asset_url}/{self.avatar}"

    def parse_orders(self, orders: List[Dict[str, Any]]) -> None:
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}

        for order_type in ['sell_orders', 'buy_orders']:
            for order in orders[order_type]:
                parsed_order = {
                    'item': order['item']['en']['item_name'],
                    'item_url_name': order['item']['url_name'],
                    'item_id': order['item']['id'],
                    'last_update': order['last_update'],
                    'quantity': order['quantity'],
                    'price': order['platinum'],
                }

                if 'subtype' in order:
                    parsed_order['subtype'] = order['subtype']

                if 'mod_rank' in order:
                    parsed_order['subtype'] = f"R{order['mod_rank']}"

                self.orders[order_type.split('_')[0]].append(parsed_order)

    def parse_reviews(self, reviews: List[Dict[str, Any]]) -> None:
        for review in reviews:
            parsed_review = {
                'user': review['user_from']['ingame_name'],
                'user_id': review['user_from']['id'],
                'user_avatar': review['user_from']['avatar'],
                'user_region': review['user_from']['region'],
                'text': review['text'],
                'date': review['date'],
            }

            if parsed_review not in self.reviews:
                self.reviews.append(parsed_review)

    async def fetch_orders(self) -> None:
        orders = await fetch_wfm_data(f"{self.base_api_url}/profile/{self.username}/orders")
        if orders is None:
            return

        self.parse_orders(orders['payload'])

    async def fetch_reviews(self, page_num: str = '1') -> None:
        reviews = await fetch_wfm_data(f"{self.base_api_url}/profile/{self.username}/reviews/{page_num}")

        if reviews is None:
            return

        self.parse_reviews(reviews['payload']['reviews'])
