import argparse
from pyarrow import parquet

def create_partitions(edge_table, partition_count):
    print(edge_table, partition_count)

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
arg_parser.add_argument(
    "--partitions",
    metavar="Number of partitions",
    type=str,
    required=True,
)
args = arg_parser.parse_args()

edge_table = parquet.read_table(f"{args.src}/edges.parquet")
create_partitions(edge_table, args.partitions)
