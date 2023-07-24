from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

import pymysql
from fuzzywuzzy import fuzz
from pymysql import Connection
from .MarketItem import MarketItem
from .MarketUser import MarketUser
from ...common import logger


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

    _FETCH_ALL_USERS_QUERY = """
        SELECT user_id, ingame_name FROM market_users
    """

    _GET_PRICE_HISTORY_QUERY = """
        SELECT datetime, avg_price
        FROM item_statistics
        WHERE item_id=%s
        AND order_type='closed'
    """

    _GET_DEMAND_HISTORY_QUERY = """
        SELECT datetime, volume
        FROM item_statistics
        WHERE item_id=%s
        AND order_type='closed'
    """

    def __init__(self, user: str, password: str, host: str, database: str) -> None:
        self.connection: Connection = pymysql.connect(user=user,
                                                      password=password,
                                                      host=host,
                                                      database=database)

        self.all_items = self._get_all_items()
        self.users: Dict[str, str] = {}

    def _execute_query(self, query: str, *params, fetch: str = 'all',
                       commit: bool = False, many: bool = False) -> Union[Tuple, List[Tuple], None]:
        self.connection.ping(reconnect=True)

        with self.connection.cursor() as cur:
            if many:
                cur.executemany(query, params[0])
            else:
                cur.execute(query, params)

            if commit:
                self.connection.commit()

            if fetch == 'one':
                return cur.fetchone()
            elif fetch == 'all':
                return cur.fetchall()

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

    async def get_user(self, user: str,
                       fetch_user_data: bool = True,
                       fetch_orders: bool = True,
                       fetch_reviews: bool = True) -> Optional[MarketUser]:
        result = self._execute_query(self._GET_CORRECT_CASE_QUERY, user, fetch='one')

        if result is None:
            # Username not found, attempt to fetch from API
            profile = await MarketUser.fetch_user_data(user)

            if profile is None:
                return None

            user_id = profile['id']
            username = profile['ingame_name']

            self.users[user_id] = username
        else:
            user_id, username = result

        # Return the correct casing
        return await MarketUser.create(self, user_id, username,
                                       fetch_user_data=fetch_user_data,
                                       fetch_orders=fetch_orders,
                                       fetch_reviews=fetch_reviews)

    async def get_item(self, item: str, fetch_orders: bool = True,
                       fetch_parts: bool = True, fetch_part_orders: bool = True,
                       fetch_price_history: bool = True, fetch_demand_history: bool = True,
                       platform: str = 'pc') -> Optional[MarketItem]:
        fuzzy_item = self._get_fuzzy_item(item)

        if fuzzy_item is None:
            return None

        item_data: List[str] = list(fuzzy_item.values())

        return await MarketItem.create(self, *item_data,
                                       fetch_parts=fetch_parts,
                                       fetch_orders=fetch_orders,
                                       fetch_part_orders=fetch_part_orders,
                                       fetch_price_history=fetch_price_history,
                                       fetch_demand_history=fetch_demand_history,
                                       platform=platform)

    def get_item_statistics(self, item_id: str) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEM_STATISTICS_QUERY, item_id, fetch='all')

    def get_item_volume(self, item_id: str, days: int = 31) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEM_VOLUME_QUERY, days, item_id, fetch='all')

    def get_item_price_history(self, item_id: str) -> Dict[str, str]:
        return dict(self._execute_query(self._GET_PRICE_HISTORY_QUERY, item_id, fetch='all'))

    def get_item_demand_history(self, item_id: str) -> Dict[str, str]:
        return dict(self._execute_query(self._GET_DEMAND_HISTORY_QUERY, item_id, fetch='all'))

    def _get_fuzzy_item(self, item_name: str) -> Optional[Dict[str, str]]:
        best_score, best_item = find_best_match(item_name, self.all_items, self.get_word_aliases())

        return best_item if best_score > 50 else None

    def get_item_parts(self, item_id: str) -> Tuple[Tuple[Any, ...], ...]:
        return self._execute_query(self._GET_ITEMS_IN_SET_QUERY, item_id, fetch='all')

    def get_word_aliases(self) -> Dict[str, str]:
        return dict(self._execute_query(self._GET_ALL_WORD_ALIASES_QUERY, fetch='all'))

    async def add_item_alias(self, item_id, alias):
        self._execute_query(self._ADD_ITEM_ALIAS_QUERY, item_id, alias, commit=True)
        self.all_items = self._get_all_items()

    def remove_item_alias(self, item_id, alias):
        self._execute_query(self._REMOVE_ITEM_ALIAS_QUERY, item_id, alias, commit=True)
        self.all_items = self._get_all_items()

    async def add_word_alias(self, word, alias):
        self._execute_query(self._ADD_WORD_ALIAS_QUERY, word, alias, commit=True)

    def update_usernames(self) -> None:
        # Fetch all user data first
        user_data = dict(self._execute_query(self._FETCH_ALL_USERS_QUERY, fetch='all'))

        # Prepare batch queries
        update_queries = []
        history_queries = []
        now = datetime.now()

        users = self.users.copy()
        self.users.clear()
        for user_id, new_ingame_name in users.items():
            current_ingame_name = user_data.get(user_id)

            # If the user doesn't exist or username is different,
            # update the user's username in `market_users` and add a record in `username_history`
            if current_ingame_name is None or new_ingame_name != current_ingame_name:
                logger.info(f"Updating username for user {user_id} to {new_ingame_name}")

                update_queries.append((user_id, new_ingame_name))
                history_queries.append((user_id, new_ingame_name, now))

        # Execute the queries
        if update_queries:
            self._execute_query(self._UPSERT_USER_QUERY, update_queries, commit=True, many=True)

        if history_queries:
            self._execute_query(self._INSERT_USERNAME_HISTORY_QUERY, history_queries, commit=True, many=True)
