import argparse
import asyncio

import modules.MarketAPI as MarketAPI
import modules.MarketDB as MarketDB
import common


async def main(args):
    if args.clear:
        await common.clear_cache()

    items, item_ids, item_info, manifest_dict = None, None, None, None
    if args.fetch is not None:
        if args.fetch == "NEW":
            items, item_ids = await MarketAPI.fetch_and_save_items_and_ids()
            item_info = await MarketAPI.fetch_and_save_statistics(items, item_ids)
            manifest_dict = await MarketAPI.get_manifest()
        else:
            items, item_ids, item_info = MarketAPI.fetch_premade_item_data()
            await MarketAPI.fetch_premade_statistics()
            manifest_dict = MarketAPI.fetch_premade_manifest()

    if args.database is not None:
        MarketDB.save_items(items, item_ids, item_info)
        MarketDB.save_items_in_set(item_info)
        MarketDB.save_item_tags(item_info)
        MarketDB.save_item_subtypes(item_info)
        MarketDB.build_and_save_category_info(manifest_dict)

        last_save_date = MarketDB.get_last_saved_date(args.database)

        MarketDB.insert_item_statistics(last_save_date)

        MarketDB.commit_data()


def parse_handler():
    parser = argparse.ArgumentParser(description="Fetch and process Warframe Market price history.")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the log level.")
    parser.add_argument("-f", "--fetch", choices=["PREMADE", "NEW"],
                        help="Fetch and process price history from the API."
                             "\nPREMADE: Fetch premade data from relics.run."
                             "\nNEW: Fetch data directly from warframe.market.")
    parser.add_argument("-d", "--database", choices=["ALL", "NEW"],
                        help="Save all price history to the database.\n"
                             "ALL: Save all price history to the database.\n"
                             "NEW: Save only new price history to the database.")
    parser.add_argument("-c", "--clear", action="store_true",
                        help="Clears the cache.")

    args = parser.parse_args()

    if args.log_level:
        common.logger.setLevel(args.log_level)

    asyncio.run(main(args))


if __name__ == "__main__":
    parse_handler()
