import csv
from itertools import islice


def csv_reader(path):
    """
    A Simple generator that opens a csv file and then reads and returns the lines one at a time.
    """
    with open(path) as input_csv:
        reader = csv.reader(input_csv)
        for row in reader:
            yield row
