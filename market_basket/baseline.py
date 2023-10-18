from .csv_util import csv_reader
import sqlite3

# Ideally we'd use itertools implementation, but
# if the python version is < 3.12 it will not be available
try:
    from itertools import batched
except:
    from utils import batched

INSERT_QUERY = "INSERT INTO baskets VALUES(?, ?)"
PRODUCT_COMBINATION_QUERY = """
SELECT first_product, second_product, COUNT() num_baskets 
FROM
(
SELECT
    p1.product_id AS first_product,
    p2.product_id AS second_product
FROM
    baskets AS p1
INNER JOIN
    baskets AS p2 ON p1.basket_id = p2.basket_id
WHERE
    p1.product_id < p2.product_id
) pairs
GROUP BY first_product, second_product;
"""


def main(input_filename, output_filename, cache_size):
    # Initialize CSV Reader
    reader = csv_reader(input_filename)

    # Connect to local sqlite db
    db_filename = input_filename.split("/")[-1].split(".")[0]
    con = sqlite3.connect(f"{db_filename}.db")

    # Get the DB cursor
    cursor = con.cursor()

    # It is assumed that each log line will be at most ~100 bytes
    # This limit is in kb
    cache_limit = (cache_size * 100) / 1000
    cursor.execute(f"PRAGMA cache_size = -{cache_limit}")

    # Initialize the db table
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS baskets(basket_id varchar(255), product_id int)"
    )

    # Write all rows to the DB in batches to avoid memory constraints
    for batch in batched(reader, cache_size):
        records = [(basket_id, product_id) for basket_id, product_id in batch]
        cursor.executemany(INSERT_QUERY, records)
        con.commit()

    # Query the table for all product combinations
    with open(output_filename, "w+") as output:
        output.write("product_1,product_2,# baskets\n")
        for row in cursor.execute(PRODUCT_COMBINATION_QUERY):
            p1, p2, value = row
            output.write(f"{p1},{p2},{value}\n")
