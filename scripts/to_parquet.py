from pyarrow import csv, parquet
import argparse

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

files = ["nodes", "edges"]
for file in files:
    content = csv.read_csv(f"{args.src}/{file}.csv")
    parquet.write_table(content, f"{args.dest}/{file}.parquet")
