from .utils import LRUCache
from .csv_util import csv_reader
from functools import partial
import os

import csv

# Assuming 32 bit ints each line will be at most 32*3 bytes + 4 for commas and linebreak
PER_ROW_BYTES = 100

# Where to dump intermediate data when it can no longer fit in memory
CACHE_DIR = "cache"


def gen_cache_filename(p1, p2, scale_factor):
    # Assumes p1/p2 are < 2**16
    # Seperate the keyspace into roughly halves for p1 and p2
    # Odd bits get assigned to p2
    p1_factor = scale_factor // 2
    p2_factor = scale_factor // 2 + scale_factor % 2
    # Shifting left is important so that progressively smaller scale factors don't collide
    p1_shard = (p1 >> p1_factor) << p1_factor
    p2_shard = (p2 >> p2_factor) << p2_factor
    # Formats an 8 digit hex string with the first 4 bytes for the p1_shard and the second for p2_shard
    return (p1_shard << 16) + p2_shard


def close_file(key, value):
    value.close()


# Cache file handles so we don't have to leave user space
def dump_cache(key, value, scale_factor):
    """
    Writes out a partial sum to the appropriate cache file.
    """
    p1, p2 = key
    cache_file = gen_cache_filename(p1, p2, scale_factor)
    with open(f"{CACHE_DIR}/{cache_file:0{8}x}", "a+") as output:
        output.write(f"{p1},{p2},{value}\n")


def process_basket(cache, items):
    """
    Generates all ordered combinations of a basket and updates cache values for each one.
    """
    items.sort()
    num_items = len(items)
    if num_items > 1:
        for x in range(num_items):
            for y in range(x + 1, num_items):
                key = (items[x], items[y])
                cache[key] = cache.get(key, 0) + 1


def bucketize_data(filename, scale_factor, cache_size):
    """
    Reads data from a csv file and groups it into baskets. Baskets are then processed into
    partial pair counts and written out into (hopefully) managably sized cache files.
    """
    # Initialize CSV Reader
    reader = csv_reader(filename)

    cache = LRUCache(cache_size, partial(dump_cache, scale_factor=scale_factor))

    items = []
    last_basket = None
    # Iterate over the rows grouping baskets as they are read
    for row in reader:
        basket, item = row
        item = int(item)

        if basket == last_basket:
            # If basket id has not changed then continue gathering items
            items.append(item)
        else:
            # If the basket id differs this order is finished and can be processed
            process_basket(cache, items)

            # Set the new basket id and populate the first item
            last_basket = basket
            items = [item]

    # Process the last group of items
    process_basket(cache, items)

    cache.flush()


def split_cache_file(filename, scale_factor, cache_size):
    """
    Reads a cache file and redistributes it based on the given scale factor.
    """
    cache_filename = f"{CACHE_DIR}/{filename}"
    temp_file_name = f"{cache_filename}.temp"

    os.rename(cache_filename, temp_file_name)

    cache = LRUCache(cache_size, partial(dump_cache, scale_factor=scale_factor))

    reader = csv_reader(temp_file_name)

    for row in reader:
        p1, p2, count = row
        key = (int(p1), int(p2))
        cache[key] = cache.get(key, 0) + int(count)

    cache.flush()
    os.remove(temp_file_name)


def split_as_required(scale_factor, cache_size):
    """
    Splits any cache files into progressively smaller buckets until they become a reasonable size.
    """
    checked = set()
    filesize_limit = cache_size * PER_ROW_BYTES

    while scale_factor >= 0:
        to_check = set(os.listdir(CACHE_DIR)) - checked
        if not to_check:
            return

        for cache_filename in to_check:
            info = os.stat(f"{CACHE_DIR}/{cache_filename}")
            size = info.st_size
            if size > filesize_limit:
                # TODO this could return the new files created for further checking
                # rather than doing a set difference each while loop iteration
                split_cache_file(cache_filename, scale_factor, cache_size)
            else:
                checked.add(cache_filename)

        scale_factor -= 1


def create_final_csv(output_filename):
    """
    Coalesces each cache file into the final csv. Previous steps should have squashed the records into cache
    files that are small enough to load into memory.
    """
    with open(output_filename, "w+") as ofile:
        ofile.write("product_1,product_2,# baskets\n")
        for cache_filename in os.listdir(CACHE_DIR):
            reader = csv_reader(f"{CACHE_DIR}/{cache_filename}")
            cache = dict()

            for row in reader:
                p1, p2, count = row
                key = (p1, p2)
                cache[key] = cache.get(key, 0) + int(count)

            for key, value in cache.items():
                p1, p2 = key
                ofile.write(f"{p1},{p2},{value}\n")


def main(input_filename, output_filename, cache_size):
    # Calculate scale factor.
    # The Scale Factor determines the maximum number of lines each cache file can be. Each cache
    # file can hold 2**scale_factor lines of data max.
    # Setting it too high will lead to cache files that are too big and need to be split. Too low
    # and the number of files starts getting unwieldly.
    scale_factor = cache_size.bit_length()
    bucketize_data(input_filename, scale_factor, cache_size)
    split_as_required(scale_factor, cache_size)
    create_final_csv(output_filename)
