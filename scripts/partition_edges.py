import argparse
import pandas as pd


def partition_edges(edge_file: str, partition_count: int, dest: str):
    """Partition an `edge_file` (parquet format) into `partition_count` files via pandas.
    It will delete existing partitioned files

    example:\n
    `dest`\n
    --shard=0\n
    ----partitioned.parquet\n
    --shard=1\n
    ----partitioned.parquet\n

    Args:
        edge_file (str): edges (.parquet)
        partition_count (int): number of partitions
        dest (str): folder that will contain partitions
    """
    edges = pd.read_parquet(edge_file)
    edges["shard"] = edges.index % partition_count
    edges.to_parquet(
        dest, partition_cols="shard", existing_data_behavior="delete_matching"
    )


if __name__ == "__main__":
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

    partition_edges(args.edges, args.partitions, args.dest)
