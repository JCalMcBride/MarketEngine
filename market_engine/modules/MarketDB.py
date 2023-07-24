import json
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Any

import pymysql
from pytz import timezone

from .categories import build_parser, get_wfm_item_categorized

@contextmanager
def managed_transaction(connection):
    try:
        connection.begin()
        yield
        connection.commit()
    except Exception as e:
        connection.rollback()
        raise e


def connect_to_database(user, password, host, database):
    return pymysql.connect(user=user, password=password, host=host, database=database)


def save_items(connection, items, item_ids, item_info):
    if items is None or item_ids is None:
        return

    data_list = [(item['id'], item['item_name'], item['url_name'], item['thumb'], item_info[item['id']]['mod_max_rank'])
                 for item in items]

    new_item_ids = {}
    for item, item_id in item_ids.items():
        if item_id not in [x['id'] for x in items]:
            new_item_ids[item] = item_id
    data_list.extend([(new_item_ids[item], item, None, None, None) for item in new_item_ids])

    insert_query = """
        INSERT IGNORE INTO items (id, item_name, url_name, thumb, max_rank)
        VALUES (%s, %s, %s, %s, %s)
    """

    with connection.cursor() as cursor:
        cursor.executemany(insert_query, data_list)

    connection.commit()


def get_sub_type_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT i.item_name, GROUP_CONCAT(DISTINCT s.sub_type) as subtypes
                          FROM item_subtypes s
                          JOIN items i ON s.item_id = i.id
                          GROUP BY i.item_name""")
        sub_type_data = cursor.fetchall()

        return {row[0]: row[1].split(',') for row in sub_type_data}


def get_item_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""SELECT item_name, id FROM items""")

    return dict(cursor.fetchall())


def build_and_save_category_info(connection, manifest_dict):
    wf_parser = build_parser(manifest_dict)
    item_categories = get_wfm_item_categorized(get_item_data(connection), manifest_dict, wf_parser)

    query = """UPDATE items SET item_type = %s WHERE id = %s"""

    with connection.cursor() as cursor:
        for item_type in item_categories:
            cursor.executemany(query, [(item_type, item_id) for item_id in item_categories[item_type].values()])


def get_all_sets(connection):
    with connection.cursor() as cursor:
        cursor.execute("""select id, url_name from items where item_name like '%Set'""")

    return dict(cursor.fetchall())


def get_last_saved_date(connection, date_to_fetch):
    if date_to_fetch == "NEW":
        return get_date(connection)
    elif date_to_fetch == "ALL":
        return None


def insert_item_statistics(connection, last_save_date, platform: str = 'pc'):
    file_list = get_file_list(last_save_date, platform)
    data_list = get_data_list(file_list)

    # Get the union of all keys in the data_list
    all_columns = set().union(*(data.keys() for data in data_list))
    columns_str = ', '.join(all_columns)
    placeholders = ', '.join(['%s'] * len(all_columns))

    insert_query = f"""
        INSERT IGNORE INTO item_statistics ({columns_str})
        VALUES ({placeholders})
    """

    # Create a list of values for each dictionary, using None for missing keys
    for data in data_list:
        if 'order_type' not in data:
            data['order_type'] = 'Closed'

    values = [tuple(data.get(key, None) for key in all_columns) for data in data_list]

    batch_size = 10_000
    total_batches = (len(values) + batch_size - 1) // batch_size

    with connection.cursor() as cursor:
        with managed_transaction(connection):
            for i in range(0, len(values), batch_size):
                batch_values = values[i:i + batch_size]
                cursor.executemany(insert_query, batch_values)
                print(f"Progress: Batch {i // batch_size + 1} of {total_batches} completed")


def open_price_history_file(filename: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    with open(filename, "r") as fp:
        return json.load(fp)


def parse_price_history(price_history):
    data_list = []
    for item in price_history:
        for statistic_type in price_history[item]:
            data_list.append(statistic_type)

    return data_list


def get_date(connection):
    query = "SELECT MAX(datetime) FROM item_statistics"

    with connection.cursor() as cursor:
        cursor.execute(query)
        most_recent_datetime = cursor.fetchone()[0]

    if most_recent_datetime is not None:
        # Convert the datetime to UTC
        utc_datetime = most_recent_datetime.astimezone(timezone('UTC'))
        most_recent_date = utc_datetime.date()
    else:
        most_recent_date = None

    return most_recent_date


def get_file_list(date, output_directory='output', platform: str = 'pc'):
    file_list = []
    for file in os.listdir(output_directory):
        if file.endswith(".json"):
            if date is not None:
                file_date = datetime.strptime(file, "price_history_%Y-%m-%d.json").date()
                if file_date <= date:
                    continue

            file_list.append(os.path.join(output_directory, file))

    return file_list


def get_data_list(file_list):
    data_list = []

    for file in file_list:
        data_list.extend(parse_price_history(open_price_history_file(file)))

    return data_list


def commit_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("COMMIT")


def save_items_in_set(connection, item_info):
    data_list = []
    for item in item_info:
        for item_in_set in item_info[item]['set_items']:
            data_list.append((item, item_in_set))

    query = """INSERT IGNORE INTO items_in_set (set_id, item_id) VALUES (%s, %s)"""
    with connection.cursor() as cursor:
        cursor.executemany(query, data_list)


def save_item_tags(connection, item_info):
    data_list = []
    for item in item_info:
        for tag in item_info[item]['tags']:
            data_list.append((item, tag))

    query = """INSERT IGNORE INTO item_tags (item_id, tag) VALUES (%s, %s)"""
    with connection.cursor() as cursor:
        cursor.executemany(query, data_list)


def save_item_subtypes(connection, item_info):
    data_list = []
    for item in item_info:
        for subtype in item_info[item]['subtypes']:
            data_list.append((item, subtype))

    query = """INSERT IGNORE INTO item_subtypes (item_id, sub_type) VALUES (%s, %s)"""
    with connection.cursor() as cursor:
        cursor.executemany(query, data_list)
