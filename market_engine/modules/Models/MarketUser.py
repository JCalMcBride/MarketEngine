class MarketUser:
    base_api_url: str = "https://api.warframe.market/v1"
    base_url: str = "https://warframe.market/profile"
    asset_url: str = "https://warframe.market/static/assets"

    def __init__(self, database: MarketDatabase, user_id: str, username: str):
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
    async def create(cls, database: MarketDatabase, user_id: str, username: str,
                     fetch_user_data: bool = True, fetch_orders: bool = True, fetch_reviews: bool = True):
        obj = cls(database, user_id, username)

        tasks = []
        if fetch_user_data:
            profile = await MarketUser.fetch_user_data(username)
            if profile is not None:
                obj.set_user_data(profile)

        if fetch_orders:
            tasks.append(obj.fetch_orders())

        if fetch_reviews:
            tasks.append(obj.fetch_reviews())

        await asyncio.gather(*tasks)

        return obj

    @staticmethod
    async def fetch_user_data(username) -> Union[None, Dict]:
        user_data = await fetch_wfm_data(f"{MarketUser.base_api_url}/profile/{username}")
        if user_data is None:
            return

        # Load the user profile

        try:
            profile = user_data['payload']['profile']
        except KeyError:
            return

        return profile

    def set_user_data(self, profile: Dict[str, Any]) -> None:
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
