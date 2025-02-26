import asyncio
from typing import List, Dict, Union, Tuple, Coroutine, Any, Optional

from ..Common import fetch_api_data, get_wfm_headers, cache_manager, session_manager
from datetime import datetime, timedelta, date

class MarketItem:
    """
    Base class for market items
    """
    base_api_url: str = "https://api.warframe.market/v1"  # Base URL for warframe.market API
    base_url: str = "warframe.market/items"  # Base URL for warframe.market items
    asset_url: str = "https://warframe.market/static/assets"  # Base URL for warframe.market assets

    def __init__(self, database: "MarketDatabase",
                 item_id: str, item_name: str, item_type: str, item_url_name: str, thumb: str, max_rank: str,
                 aliases: List, platform: str = 'pc') -> None:
        """
        Initializes a MarketItem object.
        :param database: database object
        :param item_id: the warframe.market item id
        :param item_name: the english item name
        :param item_type: the type of the item (e.g. 'Mods', 'Relics', etc.)
        :param item_url_name: the url name of the item, as used in the warframe.market url
        :param thumb: the url of the item's thumbnail
        :param max_rank: the maximum rank of the item (for mods)
        :param aliases: a list of aliases for the item
        :param platform: the platform to fetch the item for
        """
        self.database: "MarketDatabase" = database
        self.item_id: str = item_id
        self.item_name: str = item_name
        self.item_type: str = item_type
        self.item_url_name: str = item_url_name
        self.thumb: str = thumb
        self.max_rank: str = max_rank
        self.aliases: List = aliases
        self.thumb_url: str = f"{MarketItem.asset_url}/{self.thumb}"
        platform_url_string: str = f"{platform}." if platform != 'pc' else ''
        self.item_url: str = f"https://{platform_url_string}{MarketItem.base_url}/{self.item_url_name}"
        self.orders: Dict[str, List[Dict[str, Union[str, int]]]] = {'buy': [], 'sell': []}
        self.parts: List[MarketItem] = []
        self.part_orders_fetched: bool = False
        self.part_price_history_fetched: bool = False
        self.part_demand_history_fetched: bool = False
        self.price_history: Dict[str, str] = {}
        self.demand_history: Dict[str, str] = {}
        self.last_average_price = self.database.get_item_price(self.item_name)
        self.platform = platform

    @classmethod
    async def create(cls, database: "MarketDatabase", item_id: str, item_name: str, item_type: str,
                     item_url_name: str, thumb: str, max_rank: str, aliases: List, fetch_orders: bool = True,
                     fetch_parts: bool = True, fetch_part_orders: bool = True,
                     fetch_price_history: bool = True, fetch_demand_history: bool = True,
                     platform: str = 'pc') -> "MarketItem":
        """
        Creates a MarketItem object and fetches the relevant data.
        :param database: database object
        :param item_id: the warframe.market item id
        :param item_name: the english item name
        :param item_type: the type of the item (e.g. 'Mods', 'Relics', etc.)
        :param item_url_name: the url name of the item, as used in the warframe.market url
        :param thumb: the url of the item's thumbnail
        :param max_rank: the maximum rank of the item (for mods)
        :param aliases: a list of aliases for the item
        :param fetch_orders: whether to fetch the item's orders
        :param fetch_parts: whether to fetch the item's parts
        :param fetch_part_orders: whether to fetch the orders for the item's parts
        :param fetch_price_history: whether to fetch the item's price history
        :param fetch_demand_history: whether to fetch the item's demand history
        :param platform: the platform to fetch the item for
        :return: a MarketItem object
        """
        obj = cls(database, item_id, item_name, item_type, item_url_name, thumb, max_rank, aliases, platform)

        tasks = []
        if fetch_orders:
            tasks.append(obj.get_orders())

        if fetch_parts:
            obj.get_parts()

        if fetch_part_orders:
            tasks += obj.get_part_orders_tasks()

        item_ids = [obj.item_id] + [part.item_id for part in obj.parts if part is not None]

        if fetch_price_history:
            price_history = obj.database.get_item_price_history(item_ids, platform)
            obj.price_history = price_history.get(obj.item_id, {})
            for part in obj.parts:
                if part is not None:
                    part.price_history = price_history.get(part.item_id, {})

        if fetch_demand_history:
            demand_history = obj.database.get_item_demand_history(item_ids, platform)
            obj.demand_history = demand_history.get(obj.item_id, {})
            for part in obj.parts:
                if part is not None:
                    part.demand_history = demand_history.get(part.item_id, {})

        await asyncio.gather(*tasks)

        return obj

    @staticmethod
    def create_filters(**kwargs) -> Tuple[Dict[str, Union[int, str, List[int], List[str]]], Dict[str, str]]:
        """
        Creates a filters dictionary and a mode dictionary from the provided keyword arguments.
        :param kwargs: The keyword arguments to create the filters and mode dictionaries from.
        :return: A tuple containing the filters dictionary and the mode dictionary, used for filtering orders.
        """
        filters = {}
        mode = {}

        for key, value in kwargs.items():
            if key.endswith('_mode'):
                field = key[:-5]
                mode[field] = value
            else:
                filters[key] = value

        return filters, mode

    def get_subtypes(self, order_type: str = 'sell') -> List[str]:
        """
        Returns a list of the item's subtypes from the item's current orders.
        :param order_type: The type of orders to get the subtypes from ('sell' or 'buy')
        :return: A list of the item's subtypes
        """
        subtypes = []
        for order in self.orders[order_type]:
            if 'subtype' in order and order['subtype'] not in subtypes and order['state'] == 'ingame':
                subtypes.append(order['subtype'])

        return subtypes

    def get_part_orders_tasks(self) -> list[Coroutine[Any, Any, None]]:
        """
        Returns a list of tasks for fetching the orders of the item's parts.
        :return: A list of tasks for fetching the orders of the item's parts.
        """
        tasks = [part.get_orders() for part in self.parts if part is not None]
        self.part_orders_fetched = True

        return tasks

    def get_price_history(self) -> None:
        """
        Fetches the item's price history from the database.
        :return: None
        """
        self.price_history = self.database.get_item_price_history(self.item_id, self.platform)

    def get_demand_history(self) -> None:
        """
        Fetches the item's demand history from the database.
        :return: None
        """
        self.demand_history = self.database.get_item_demand_history(self.item_id, self.platform)

    def add_alias(self, alias: str) -> None:
        """
        Adds an alias to the item.
        :param alias: The alias to add.
        :return: None
        """
        self.database.add_item_alias(self.item_id, alias)

    def remove_alias(self, alias: str) -> None:
        """
        Removes an alias from the item.
        :param alias: The alias to remove.
        :return: None
        """
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

    async def parse_orders(self, orders: List[Dict[str, Any]]) -> None:
        """
        Parses the orders fetched from the warframe.market API.
        :param orders: The orders fetched from the warframe.market API.
        :return: None
        """
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
            # First sort by 'last_update' in descending order
            self.orders[key] = sorted(self.orders[key], key=lambda x: x['last_update'], reverse=True)
            # Then sort by 'price' according to the 'reverse' flag
            self.orders[key] = sorted(self.orders[key], key=lambda x: x['price'], reverse=reverse)

        self.database.users.update(users)

    async def get_orders(self) -> None:
        """
        Fetches the item's orders from the warframe.market API.
        :return: None
        """
        async with cache_manager() as cache, session_manager() as session:
            orders = await fetch_api_data(cache=cache,
                                          session=session,
                                          url=f"{self.base_api_url}/items/{self.item_url_name}/orders",
                                          headers=get_wfm_headers(platform=self.platform),
                                          expiration=20)
            if orders is None:
                return

            await self.parse_orders(orders['payload']['orders'])

    def get_parts(self) -> None:
        """
        Fetches the item's parts from the database.
        :return: None
        """
        self.parts = [MarketItem(self.database, *item) for item in self.database.get_item_parts(self.item_id)]

    def calculate_volume_statistics(self):
        now = date.today()
        day_ago = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        year_ago = now - timedelta(days=365)

        day_total = week_total = month_total = year_total = 0
        day_count = week_count = month_count = year_count = 0

        for date_obj, volume in self.demand_history.items():
            if isinstance(date_obj, str):
                date_obj = datetime.strptime(date_obj, '%Y-%m-%d').date()
            elif isinstance(date_obj, datetime):
                date_obj = date_obj.date()

            volume = int(volume)

            if date_obj >= day_ago:
                day_total += volume
                day_count += 1
            if date_obj >= week_ago:
                week_total += volume
                week_count += 1
            if date_obj >= month_ago:
                month_total += volume
                month_count += 1
            if date_obj >= year_ago:
                year_total += volume
                year_count += 1

        week_average = week_total / week_count if week_count else 0
        month_average = month_total / month_count if month_count else 0
        year_average = year_total / year_count if year_count else 0

        return {
            'day_total': day_total,
            'week_total': week_total,
            'month_total': month_total,
            'year_total': year_total,
            'week_average': round(week_average, 2),
            'month_average': round(month_average, 2),
            'year_average': round(year_average, 2)
        }

    def to_dict(self) -> Dict[str, Any]:
        volume_stats = self.calculate_volume_statistics()
        return {
            'item_id': self.item_id,
            'item_name': self.item_name,
            'item_type': self.item_type,
            'item_url_name': self.item_url_name,
            'thumb': self.thumb,
            'thumb_url': self.thumb_url,
            'max_rank': self.max_rank,
            'aliases': self.aliases,
            'item_url': self.item_url,
            'platform': self.platform,
            'orders': self.orders,
            'parts': [part.to_dict() if part else None for part in self.parts],
            'last_average_price': self.last_average_price,
            'price_history': self._convert_history_to_str(self.price_history),
            'demand_history': self._convert_history_to_str(self.demand_history),
            'volume_statistics': volume_stats
        }

    def _convert_history_to_str(self, history: Dict[datetime, str]) -> Dict[str, str]:
        return {date.date().isoformat(): value for date, value in history.items()}

