import argparse
import pandas as pd

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument(
    "--edges",  
    metavar="Parquet file containing edges to partitions",
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
    type=int,
    required=True,
)
args = arg_parser.parse_args()

def create_partitions(edge_file: str, partition_count: int, dest: str):
    edges = pd.read_parquet(edge_file)
    edges["shard"] = edges.index % partition_count
    edges.to_parquet(dest, partition_cols="shard")
    
create_partitions(args.edges, args.partitions, args.dest)
