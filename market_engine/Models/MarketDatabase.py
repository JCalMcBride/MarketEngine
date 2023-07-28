import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

import pymysql
from fuzzywuzzy import fuzz
from pymysql import Connection
from pytz import timezone

from .MarketItem import MarketItem
from .MarketUser import MarketUser
from market_engine.Common import logger, get_statistic_path


def get_item_names(item: Dict[str, Any]) -> List[str]:
    """
    Returns a list of all names for an item, including aliases
    :param item: item dictionary for an item
    :return: list of all names for an item
    """
    return [item['item_name']] + item.get('aliases', [])


def closest_common_word(word: str, common_words: set, threshold: int) -> Optional[str]:
    """
    Returns the closest common word to the given word, if the score is above the threshold
    :param word: word to compare
    :param common_words: set of common words to compare against
    :param threshold: minimum score for a match
    :return: closest common word if the score is above the threshold, otherwise None
    """
    best_match, best_score = None, 0
    for common_word in common_words:
        score = fuzz.ratio(word, common_word)
        if score > best_score:
            best_match, best_score = common_word, score

    return best_match if best_score >= threshold else None


def remove_common_words(name: str, common_words: set) -> str:
    """
    Removes common words from a name
    :param name: name to remove common words from
    :param common_words: set of common words to remove
    :return: name with common words removed
    """
    name = remove_blueprint(name)
    threshold = 80  # Adjust this value based on the desired level of fuzzy matching

    ignore_list = ['primed']

    words = name.split()
    filtered_words = [word for word in words if not (closest_common_word(word, common_words, threshold)
                                                     and word not in ignore_list)]
    return ' '.join(filtered_words)


def remove_blueprint(s: str) -> str:
    """
    Removes the word 'blueprint' from the end of a string when it is preceded by a warframe/archwing part name.
    :param s: string to remove 'blueprint' from
    :return: string with 'blueprint' removed
    """
    words = s.lower().split()
    part_word_list = ['chassis', 'neuroptics', 'systems', 'wings', 'harness']
    if words[-1:] == ['blueprint'] and words[-2] in part_word_list:
        return ' '.join(words[:-1])
    return s.lower()


def replace_aliases(name: str, aliases: dict, threshold=80) -> str:
    """
    Replaces words in a name with their aliases
    :param name: the name to replace words in
    :param aliases: a dictionary of aliases
    :param threshold: the minimum score for a match
    :return:
    """
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


def find_best_match(item_name: str, items: List[Dict[str, Any]],
                    word_aliases: List) -> Tuple[int, Optional[Dict[str, str]]]:
    """
    Finds the best match for an item name from a list of items
    :param item_name: the name of the item to find a match for
    :param items: the list of items to search
    :param word_aliases: a list of word aliases
    :return: a tuple containing the best score and the best match
    """
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


def open_price_history_file(filename: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Opens a price history file and returns the data
    :param filename: the name of the file to open
    :return:
    """
    with open(filename, "r") as fp:
        return json.load(fp)


def parse_price_history(price_history: Dict[str, Dict[str, List[Dict[str, Any]]]]):
    """
    Parses price history data into a list of dictionaries
    :param price_history: the price history data to parse
    :return:
    """
    data_list = []
    for item in price_history:
        for statistic_type in price_history[item]:
            data_list.append(statistic_type)

    return data_list


def get_file_list(date: datetime, platform: str = 'pc'):
    """
    Gets a list of price history files
    :param date: the date to fetch files after
    :param platform: the platform to fetch files for
    :return: list of price history files after the given date
    """
    file_list = []
    output_directory = get_statistic_path(platform)

    os.makedirs(output_directory, exist_ok=True)  # Create directory if it does not exist

    for file in os.listdir(output_directory):
        if file.endswith(".json"):
            if date is not None:
                file_date = datetime.strptime(file, "price_history_%Y-%m-%d.json").date()
                if file_date <= date:
                    continue

            file_list.append(os.path.join(output_directory, file))

    return file_list


def get_data_list(file_list: List[str]):
    """
    Gets a list of data from a list of price history files
    :param file_list: the list of files to get data from
    :return: list of data from the given files
    """
    data_list = []

    for file in file_list:
        data_list.extend(parse_price_history(open_price_history_file(file)))

    return data_list


class MarketDatabase:
    """
    Class for interacting with the database
    """
    _GET_ITEM_QUERY: str = """
    SELECT * 
    FROM items 
    WHERE item_name=%s
    """

    _GET_ITEM_SUBTYPES_QUERY: str = """
    SELECT * 
    FROM item_subtypes 
    WHERE item_id=%s"""

    _GET_ITEM_STATISTICS_QUERY: str = """
    SELECT datetime, avg_price 
    FROM item_statistics 
    WHERE item_id=%s 
    AND order_type='closed' 
    AND platform=%s
    """

    _GET_ITEM_VOLUME_QUERY: str = """
    SELECT volume 
    FROM item_statistics 
    WHERE datetime >= NOW() - INTERVAL %s DAY 
    AND order_type='closed' 
    AND item_id = %s 
    AND platform=%s
    """

    _BASE_ITEMS_QUERY: str = """
    SELECT items.id, items.item_name, items.item_type, items.url_name, 
    items.thumb, items.max_rank, GROUP_CONCAT(item_aliases.alias) AS aliases
    """

    _GET_ALL_ITEMS_QUERY: str = f"""
    {_BASE_ITEMS_QUERY} 
    FROM items 
    LEFT JOIN item_aliases ON items.id = item_aliases.item_id 
    GROUP BY items.id
    """

    _GET_ITEMS_IN_SET_QUERY: str = f"""
    {_BASE_ITEMS_QUERY}
    FROM items_in_set
    INNER JOIN items
    ON items_in_set.item_id = items.id
    LEFT JOIN item_aliases ON items.id = item_aliases.item_id
    WHERE items_in_set.set_id = %s
    GROUP BY items.id, items.item_name, items.item_type, items.url_name, items.thumb, items.max_rank
    """
    _GET_USER_QUERY = "SELECT ingame_name FROM market_users WHERE user_id=%s"""
    _GET_ALL_WORD_ALIASES_QUERY: str = "SELECT alias, word FROM word_aliases"
    _GET_CORRECT_CASE_QUERY = """
        SELECT user_id, ingame_name
        FROM market_users 
        WHERE LOWER(ingame_name) = LOWER(%s)
    """
    _GET_SUBTYPE_DATA_QUERY = """SELECT i.item_name, GROUP_CONCAT(DISTINCT s.sub_type) as subtypes
                          FROM item_subtypes s
                          JOIN items i ON s.item_id = i.id
                          GROUP BY i.item_name"""

    _GET_ALL_USERS_QUERY = """
        SELECT user_id, ingame_name FROM market_users
    """

    _GET_PRICE_HISTORY_QUERY = """
        SELECT datetime, avg_price
        FROM item_statistics
        WHERE item_id=%s
        AND order_type='closed'
        AND platform=%s
    """

    _GET_DEMAND_HISTORY_QUERY = """
        SELECT datetime, volume
        FROM item_statistics
        WHERE item_id=%s
        AND order_type='closed'
        AND platform=%s
    """

    _GET_MOST_RECENT_DATE_QUERY = """
    SELECT MAX(datetime) FROM item_statistics where platform = %s
    """

    _GET_ITEM_DICT_QUERY = """
    SELECT item_name, id FROM items
    """

    _GET_ALL_SETS_QUERY = """
    SELECT id, url_name 
    FROM items 
    WHERE item_name 
    LIKE '%Set'
    """

    _ADD_ITEM_ALIAS_QUERY: str = """
    INSERT INTO item_aliases (item_id, alias) 
    VALUES (%s, %s)
    """

    _REMOVE_ITEM_ALIAS_QUERY: str = """
    DELETE FROM item_aliases 
    WHERE item_id=%s 
    AND alias=%s
    """

    _ADD_WORD_ALIAS_QUERY: str = """
    INSERT INTO word_aliases (word, alias) 
    VALUES (%s, %s)
    """

    _UPSERT_USER_QUERY = """
        INSERT INTO market_users (user_id, ingame_name) 
        VALUES (%s, %s) 
        ON DUPLICATE KEY UPDATE ingame_name=VALUES(ingame_name)
    """

    _INSERT_ITEM_QUERY: str = """
        INSERT IGNORE INTO items (id, item_name, url_name, thumb, max_rank)
        VALUES (%s, %s, %s, %s, %s)
    """

    _INSERT_USERNAME_HISTORY_QUERY = """
        INSERT INTO username_history (user_id, ingame_name, datetime) 
        VALUES (%s, %s, %s)
    """

    _INSERT_ITEM_TAGS_QUERY = """
    INSERT IGNORE INTO item_tags (item_id, tag) 
    VALUES (%s, %s)
    """

    _INSERT_ITEM_SUBTYPE_QUERY = """
    INSERT IGNORE INTO item_subtypes (item_id, sub_type) 
    VALUES (%s, %s)
    """

    _INSERT_ITEMS_IN_SET_QUERY = """INSERT IGNORE INTO items_in_set (set_id, item_id) 
    VALUES (%s, %s)
    """

    _SET_ITEM_CATEGORIES_QUERY = """
    UPDATE items 
    SET item_type = %s 
    WHERE id = %s"""

    def __init__(self, user: str, password: str, host: str, database: str) -> None:
        """
        Initializes the database
        :param user: the username to connect to the database with
        :param password: the password to connect to the database with
        :param host: the host to connect to the database with
        :param database: the database to connect to
        """
        self.connection: Connection = pymysql.connect(user=user,
                                                      password=password,
                                                      host=host,
                                                      database=database)

        self.all_items = self.get_all_items()
        self.users: Dict[str, str] = {}

    def execute_query(self, query: str, *params, fetch: str = 'all',
                      commit: bool = False, many: bool = False) -> Union[Tuple, List[Tuple], None]:
        """
        Executes a query on the database, and returns the result if applicable
        :param query: the query to execute
        :param params: the parameters to pass to the query, accepts multiple parameters
        :param fetch: the type of fetch to perform, either 'one' or 'all'
        :param commit: whether or not to commit the query
        :param many: whether or not to execute the query with multiple parameters
        :return: the result of the query if applicable
        """
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

    def get_all_items(self) -> List[dict]:
        """
        Gets all items from the database
        :return: a list of all items
        """
        all_data = self.execute_query(self._GET_ALL_ITEMS_QUERY)

        all_items: List[dict] = []
        for item_id, item_name, item_type, url_name, thumb, max_rank, alias in all_data:
            aliases = []
            if alias:
                aliases = alias.split(',')

            all_items.append({'id': item_id, 'item_name': item_name, 'item_type': item_type,
                              'url_name': url_name, 'thumb': thumb, 'max_rank': max_rank, 'aliases': aliases})

        return all_items

    def save_items(self, items, item_ids, item_info) -> None:
        """
        Saves items to the database
        :param items: dictionary of items to save
        :param item_ids: dictionary of item names to item ids
        :param item_info: dictionary of item info
        :return: None
        """
        if any(x is None for x in [items, item_ids, item_info]):
            return

        data_list = [
            (item['id'], item['item_name'], item['url_name'], item['thumb'],
             item_info[item['id']]['mod_max_rank'])
            for item in items if item['id'] in item_info]

        new_item_ids = {}
        for item, item_id in item_ids.items():
            if item_id not in [x['id'] for x in items]:
                new_item_ids[item] = item_id
        data_list.extend([(new_item_ids[item], item, None, None, None) for item in new_item_ids])

        if len(data_list) > 0:
            self.execute_query(self._INSERT_ITEM_QUERY, data_list, many=True, commit=True)

    def save_item_tags(self, item_info: Dict[str, Any]) -> None:
        """
        Saves item tags to the database
        :param item_info: dictionary of item info
        :return: None
        """
        if item_info is None:
            return None

        data_list = []
        for item in item_info:
            for tag in item_info[item]['tags']:
                data_list.append((item, tag))

        if len(data_list) > 0:
            self.execute_query(self._INSERT_ITEM_TAGS_QUERY, data_list, many=True, commit=True)

    def save_item_subtypes(self, item_info: Dict[str, Any]) -> None:
        """
        Saves item subtypes to the database
        :param item_info: dictionary of item info
        :return: None
        """
        if item_info is None:
            return None

        data_list = []
        for item in item_info:
            for subtype in item_info[item]['subtypes']:
                data_list.append((item, subtype))

        if len(data_list) > 0:
            self.execute_query(self._INSERT_ITEM_SUBTYPE_QUERY, data_list, many=True, commit=True)

    def save_items_in_set(self, item_info: Dict[str, Any]) -> None:
        """
        Saves items in a set to the database
        :param item_info: dictionary of item info
        :return: None
        """
        if item_info is None:
            return None

        data_list = []
        for item in item_info:
            for item_in_set in item_info[item]['set_items']:
                data_list.append((item, item_in_set))

        if len(data_list) > 0:
            self.execute_query(self._INSERT_ITEMS_IN_SET_QUERY, data_list, many=True, commit=True)

    def get_sub_type_data(self) -> Dict[str, List[str]]:
        """
        Gets sub type data from the database
        :return: dictionary of item names to sub types
        """
        sub_type_data = self.execute_query(self._GET_SUBTYPE_DATA_QUERY)

        return {row[0]: row[1].split(',') for row in sub_type_data}

    def get_item_id_dict(self) -> Dict[str, str]:
        """
        Gets item data from the database
        :return: dictionary of item names to item ids
        """
        return dict(self.execute_query(self._GET_ITEM_DICT_QUERY))

    def get_all_sets(self) -> Dict[str, str]:
        """
        Gets all sets from the database
        :return: dictionary of set ids to set names
        """
        return dict(self.execute_query(self._GET_ALL_SETS_QUERY))

    def save_item_categories(self, item_categories: Dict[str, Dict[str, str]]) -> None:
        """
        Saves item categories to the database
        :param item_categories: dictionary of item types to item ids
        :return: None
        """
        if item_categories is None:
            return None

        for item_type in item_categories:
            self.execute_query(self._SET_ITEM_CATEGORIES_QUERY,
                               [(item_type, item_id) for item_id in item_categories[item_type].values()],
                               many=True, commit=True)

    def insert_item_statistics(self, last_save_date: datetime = None,
                               platform: str = 'pc') -> None:
        """
        Inserts item statistics into the database
        :param last_save_date: the date to fetch files after
        :param platform: the platform to fetch files for
        :return: None
        """
        file_list = get_file_list(last_save_date, platform)
        data_list = get_data_list(file_list)

        if len(data_list) == 0:
            return

        # Get the union of all keys in the data_list
        all_columns = set().union(*(data.keys() for data in data_list))
        columns_str = ', '.join(all_columns)
        placeholders = ', '.join(['%s'] * len(all_columns))
        columns_str += ', platform'
        placeholders += ', %s'

        insert_query = f"""
            INSERT IGNORE INTO item_statistics ({columns_str})
            VALUES ({placeholders})
        """

        # Create a list of values for each dictionary, using None for missing keys
        for data in data_list:
            if 'order_type' not in data:
                data['order_type'] = 'closed'

        values = [tuple(data.get(key, None) for key in all_columns) + (platform,) for data in data_list]

        batch_size = 10_000
        total_batches = (len(values) + batch_size - 1) // batch_size

        with self.connection.cursor() as cursor:
            for i in range(0, len(values), batch_size):
                batch_values = values[i:i + batch_size]
                cursor.executemany(insert_query, batch_values)
                logger.info(f"Progress: Batch {i // batch_size + 1} of {total_batches} completed")
            self.connection.commit()

    def get_most_recent_statistic_date(self, platform: str = 'pc') -> Optional[datetime]:
        """
        Gets the most recent date in the database
        :param platform: the platform to fetch data for
        :return: the most recent date in the database for the given platform if applicable, otherwise None
        """
        most_recent_datetime: datetime = self.execute_query(self._GET_MOST_RECENT_DATE_QUERY, platform, fetch='one')[0]

        most_recent_date = None
        if most_recent_datetime is not None:
            # Convert the datetime to UTC
            utc_datetime = most_recent_datetime.astimezone(timezone('UTC'))
            most_recent_date = utc_datetime.date()

        return most_recent_date

    async def get_user(self, user: str,
                       fetch_user_data: bool = True,
                       fetch_orders: bool = True,
                       fetch_reviews: bool = True) -> Optional[MarketUser]:
        """
        Gets a user from the database
        :param user: the user to get
        :param fetch_user_data: whether or not to fetch user data from the API
        :param fetch_orders: whether or not to fetch orders from the API
        :param fetch_reviews: whether or not to fetch reviews from the API
        :return: the user if applicable, otherwise None
        """
        result = self.execute_query(self._GET_CORRECT_CASE_QUERY, user, fetch='one')

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
        """
        Gets an item from the database
        :param item: the item to get
        :param fetch_orders: whether or not to fetch orders from the API
        :param fetch_parts: whether or not to fetch parts from the API
        :param fetch_part_orders: whether or not to fetch part orders from the API
        :param fetch_price_history: whether or not to fetch price history from the database
        :param fetch_demand_history: whether or not to fetch demand history from the database
        :param platform: the platform to fetch data for
        :return: the item if applicable, otherwise None
        """
        fuzzy_item = self._get_fuzzy_item(item)  # Get the best match for the item name

        if fuzzy_item is None:  # No match found
            return None

        item_data: List[str] = list(fuzzy_item.values())  # Get the item data from the dictionary

        # Creates the item object, fetching data from the API if applicable
        return await MarketItem.create(self, *item_data,
                                       fetch_parts=fetch_parts,
                                       fetch_orders=fetch_orders,
                                       fetch_part_orders=fetch_part_orders,
                                       fetch_price_history=fetch_price_history,
                                       fetch_demand_history=fetch_demand_history,
                                       platform=platform)

    def get_item_statistics(self, item_id: str, platform: str = 'pc') -> Tuple[Tuple[Any, ...], ...]:
        """
        Gets item statistics from the database
        :param item_id: the item to get statistics for
        :param platform: the platform to fetch data for
        :return: the item statistics
        """
        return self.execute_query(self._GET_ITEM_STATISTICS_QUERY, item_id, platform, fetch='all')

    def get_item_volume(self, item_id: str, days: int = 31, platform: str = 'pc') -> Tuple[Tuple[Any, ...], ...]:
        """
        Gets item volume from the database
        :param item_id: the item to get volume for
        :param days: the number of days to get volume for
        :param platform: the platform to fetch data for
        :return: the item volume
        """
        return self.execute_query(self._GET_ITEM_VOLUME_QUERY, days, item_id, platform, fetch='all')

    def get_item_price_history(self, item_id: str, platform: str = 'pc') -> Dict[str, str]:
        """
        Gets item price history from the database
        :param item_id: the item to get price history for
        :param platform: the platform to fetch data for
        :return: the item price history
        """
        return dict(self.execute_query(self._GET_PRICE_HISTORY_QUERY, item_id, platform, fetch='all'))

    def get_item_demand_history(self, item_id: str, platform: str = 'pc') -> Dict[str, str]:
        """
        Gets item demand history from the database
        :param item_id: the item to get demand history for
        :param platform: the platform to fetch data for
        :return: the item demand history
        """
        return dict(self.execute_query(self._GET_DEMAND_HISTORY_QUERY, item_id, platform, fetch='all'))

    def _get_fuzzy_item(self, item_name: str) -> Optional[Dict[str, str]]:
        """
        Gets the best match for an item name from the database
        :param item_name: the item name to get a match for
        :return: the best match if applicable, otherwise None
        """
        best_score, best_item = find_best_match(item_name, self.all_items, self.get_word_aliases())

        return best_item if best_score > 50 else None

    def get_item_parts(self, item_id: str) -> Tuple[Tuple[Any, ...], ...]:
        """
        Gets item parts from the database
        :param item_id: the item to get parts for
        :return: the item parts
        """
        return self.execute_query(self._GET_ITEMS_IN_SET_QUERY, item_id, fetch='all')

    def get_word_aliases(self) -> Dict[str, str]:
        """
        Gets word aliases from the database
        :return: the word aliases
        """
        return dict(self.execute_query(self._GET_ALL_WORD_ALIASES_QUERY, fetch='all'))

    async def add_item_alias(self, item_id: str, alias: str) -> None:
        """
        Adds an alias to an item
        :param item_id: the item to add an alias to
        :param alias: the alias to add
        :return: None
        """
        self.execute_query(self._ADD_ITEM_ALIAS_QUERY, item_id, alias, commit=True)
        self.all_items = self.get_all_items()  # Update the list of all items

    def remove_item_alias(self, item_id: str, alias: str) -> None:
        self.execute_query(self._REMOVE_ITEM_ALIAS_QUERY, item_id, alias, commit=True)
        self.all_items = self.get_all_items()  # Update the list of all items

    async def add_word_alias(self, word: str, alias: str) -> None:
        """
        Adds an alias to a word, used for fuzzy matching
        :param word: the word to add an alias to
        :param alias: the alias to add
        :return: None
        """
        self.execute_query(self._ADD_WORD_ALIAS_QUERY, word, alias, commit=True)

    def update_usernames(self) -> None:
        """
        Updates usernames in the database
        :return: None
        """
        # Fetch all user data first
        user_data = dict(self.execute_query(self._GET_ALL_USERS_QUERY, fetch='all'))

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
            self.execute_query(self._UPSERT_USER_QUERY, update_queries, commit=True, many=True)

        if history_queries:
            self.execute_query(self._INSERT_USERNAME_HISTORY_QUERY, history_queries, commit=True, many=True)
