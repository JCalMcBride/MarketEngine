import argparse
import asyncio

import modules.MarketAPI as MarketAPI
import modules.MarketDB as MarketDB
import common


async def main(args):
    async with common.cache_manager() as cache:
        async with common.session_manager() as session:
            if args.fetch:
                await MarketAPI.fetch_and_save_statistics(cache, session)
                await MarketAPI.fetch_and_save_sets(cache, session)

            last_save_date = None
            if args.database:
                MarketDB.get_last_saved_date()

            MarketDB.insert_item_statistics(last_save_date)

            manifest_list = await MarketAPI.get_manifest(cache, session)
            MarketDB.misc_data_handler(args, manifest_list)

            MarketDB.commit_data()


def parse_handler():
    parser = argparse.ArgumentParser(description="Fetch and process Warframe Market price history.")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the log level.")
    parser.add_argument("-f", "--fetch", action="store_true", help="Fetch and process price history from the API.")
    parser.add_argument("-d", "--database", action="store_true", help="Save all price history to the database.")

    args = parser.parse_args()

    if args.log_level:
        common.logger.setLevel(args.log_level)

    asyncio.run(main(args))


if __name__ == "__main__":
    parse_handler()
