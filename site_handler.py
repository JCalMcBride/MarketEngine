from __future__ import annotations

import asyncio
import json

from modules import MarketAPI


async def main():
    items, item_ids = await MarketAPI.fetch_and_save_items_and_ids()
    price_history_dict, item_info = await MarketAPI.fetch_and_save_statistics(items, item_ids)
    manifest_dict = await MarketAPI.get_manifest()

    with open("/var/www/html/market_data/item_info.json", "w") as file:
        json.dump(item_info, file, indent=4)

    with open("/var/www/html/market_data/item_ids.json", "w") as file:
        json.dump(item_ids, file, indent=4)

    with open("/var/www/html/market_data/items.json", "w") as file:
        json.dump(items, file, indent=4)

    with open("/var/www/html/market_data/manifest.json", "w") as file:
        json.dump(manifest_dict, file, indent=4)

    MarketAPI.save_price_history(price_history_dict, directory="/var/www/html/history/")


if __name__ == "__main__":
    asyncio.run(main())
