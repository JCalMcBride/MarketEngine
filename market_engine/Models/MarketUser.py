import asyncio
from typing import Dict, List, Union, Any

from ..Common import fetch_api_data, cache_manager, session_manager


class MarketUser:
    """
    Represents a user on warframe.market
    """
    base_api_url: str = "https://api.warframe.market/v1"  # Base URL for warframe.market API
    base_url: str = "https://warframe.market/profile"  # Base URL for warframe.market profiles
    asset_url: str = "https://warframe.market/static/assets"  # Base URL for warframe.market assets

    def __init__(self, database: "MarketDatabase", user_id: str, username: str):
        """
        Initializes a MarketUser object.
        :param database: database object
        :param user_id: the user's warframe.market id
        :param username: the user's warframe.market username
        """
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
        self.reviews: List[str] = []

    @classmethod
    async def create(cls, database: "MarketDatabase", user_id: str, username: str,
                     fetch_user_data: bool = True, fetch_orders: bool = True, fetch_reviews: bool = True,
                     review_page_nums: Union[int, List[int]] = 1):
        """
        Creates a MarketUser object.
        :param database: database object
        :param user_id: the user's warframe.market id
        :param username: the user's warframe.market username
        :param fetch_user_data: whether or not to fetch the user's data from warframe.market
        :param fetch_orders: whether or not to fetch the user's orders from warframe.market
        :param fetch_reviews: whether or not to fetch the user's reviews from warframe.market
        :param review_page_nums: the page number(s) to fetch reviews from (integer or list of integers)
        :return: MarketUser object with the specified data
        """
        obj = cls(database, user_id, username)

        tasks = []
        if fetch_user_data:
            profile = await MarketUser.fetch_user_data(username)
            if profile is not None:
                obj.set_user_data(profile)
            else:
                return None

        if fetch_orders:
            tasks.append(obj.fetch_orders())

        if fetch_reviews:
            tasks.append(obj.fetch_reviews(review_page_nums))

        await asyncio.gather(*tasks)

        return obj

    @staticmethod
    async def fetch_user_data(username) -> Union[None, Dict]:
        """
        Fetches a user's data from warframe.market
        :param username: the user's warframe.market username, case-sensitive
        :return: the user's data
        """
        async with cache_manager() as cache, session_manager() as session:
            user_data = await fetch_api_data(session=session,
                                             cache=cache,
                                             url=f"{MarketUser.base_api_url}/profile/{username}")

        if user_data is None:
            return

        # Load the user profile
        try:
            profile = user_data['payload']['profile']
        except KeyError:
            return

        return profile

    def set_user_data(self, profile: Dict[str, Any]) -> None:
        """
        Sets the user's data based on the values returned from the API.
        :param profile: the user's profile data
        :return: None
        """
        for key, value in profile.items():
            if hasattr(self, key):
                setattr(self, key, value)

        if self.avatar is not None:
            self.avatar_url = f"{MarketUser.asset_url}/{self.avatar}"

    def parse_orders(self, orders: List[Dict[str, Any]]) -> None:
        """
        Parses the user's orders.
        :param orders: the user's orders from the API
        :return: None
        """
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
        """
        Parses the user's reviews.
        :param reviews: the user's reviews from the API
        :return: None
        """
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
        """
        Fetches the user's orders from warframe.market
        :return: None
        """
        async with cache_manager() as cache, session_manager() as session:
            orders = await fetch_api_data(cache=cache,
                                          session=session,
                                          url=f"{self.base_api_url}/profile/{self.username}/orders",
                                          expiration=60)

        if orders is None:
            return

        self.parse_orders(orders['payload'])

    async def fetch_reviews(self, page_nums: Union[int, List[int]]) -> None:
        """
        Fetches the user's reviews from warframe.market for the specified page numbers.
        :param page_nums: the page number(s) to fetch (integer or list of integers)
        :return: None
        """
        if isinstance(page_nums, int):
            page_nums = [page_nums]

        tasks = []
        async with session_manager() as session, cache_manager() as cache:
            for page_num in page_nums:
                url = f"{self.base_api_url}/profile/{self.username}/reviews/{page_num}"
                tasks.append(fetch_api_data(session=session,
                                            url=url,
                                            cache=cache,
                                            expiration=60))

            results = await asyncio.gather(*tasks)

            for reviews in results:
                if reviews is not None:
                    self.parse_reviews(reviews['payload']['reviews'])

    def to_dict(self):
        """
        Convert the MarketUser object to a dictionary suitable for JSON serialization.
        """
        user_dict = {
            'user_id': self.user_id,
            'username': self.username,
            'profile_url': self.profile_url,
            'last_seen': self.last_seen,
            'avatar': self.avatar,
            'avatar_url': self.avatar_url,
            'locale': self.locale,
            'background': self.background,
            'about': self.about,
            'reputation': self.reputation,
            'platform': self.platform,
            'banned': self.banned,
            'status': self.status,
            'region': self.region,
            'orders': self.orders,
            'reviews': self.reviews
        }
        return user_dict