import pandas as pd
import argparse


def to_parquet(src: str, dest: str):
    """Transform `src`/edges.csv and `src`/nodes.csv to parquet format via pandas.

    Args:
        src (str): folder containing edges.csv and nodes.csv
        dest (str): folder that will contain edges.parquet and nodes.parquet
    """

    files = ["nodes", "edges"]
    for file in files:
        content = pd.read_csv(f"{src}/{file}.csv")
        content.to_parquet(f"{dest}/{file}.parquet")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--src",  # in is a reserved keyword
        metavar="Source folder",
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--dest",
        metavar="Destination folder",
        type=str,
        required=True,
    )
    args = arg_parser.parse_args()
    to_parquet(args.src, args.dest)
