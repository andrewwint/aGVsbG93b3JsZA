from market_basket.baseline import main as baseline_main
from market_basket.task import main as task_main
import argparse

# Experimentaly 10k lines is ~1-2 MB of memory depending on the size of the integers stored
# With other Python overhead this leads to 10-15mb of memory used
CACHE_SIZE = 10000

INPUT_FILENAME = "data/data_1.csv"
OUTPUT_FILENAME = "output.csv"

APPROACH = {"baseline": baseline_main, "task": task_main}


def create_arg_parser():
    parser = argparse.ArgumentParser(
        prog="main.py",
        usage="Program entrypoint for the crisistextline market basket task.",
    )
    parser.add_argument(
        "-i", "--input", default=INPUT_FILENAME, help="The data file to read."
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_FILENAME,
        help="The file to store the final csv data in.",
    )
    parser.add_argument(
        "-c",
        "--cache-size",
        default=CACHE_SIZE,
        type=int,
        help="""
        The rough number of lines that can be held in memory at one time.
        """,
    )
    parser.add_argument(
        "-a",
        "--approach",
        default="task",
        help="""Which approach to run. `baseline` for SQL baseline approach. `task`
        for the approach that follows the task parameters.""",
    )
    return parser


if __name__ == "__main__":
    parser = create_arg_parser()
    args = parser.parse_args()
    assert (
        args.approach in APPROACH
    ), f"Approach must be one of: {list(APPROACH.keys())}"
    APPROACH[args.approach](args.input, args.output, args.cache_size)
