### GOAL:

# ● Nodes : 1 million d'entités (Person, Org, Paper) avec id, label, name
# ● Edges : 5 millions de relations REL entre nodes

# Utilisation:
# python3 scripts/generate_sample_data.py --out data/raw --nodes 1000000 --edges 5000000
# --out: répertoire de sortie
# --nodes: nombre de node
# --edges: nombre de edge

# Fichiers de sortie:
# nodes.csv
# id,label,name
# 0,Person,name_0
# 1,Org,name_1

# edges.csv
# id,label,name
# src,dst,type
# 0,123,REL
# 1,456,REL

# TODO: ajout doublon
# TODO: ajout valeur None

import math
import argparse
import csv


def generate_nodes(node_count: int) -> list[dict]:
    possible_entities = ["Person", "Org", "Paper"]
    entity_index = 0
    nodes = []
    for i in range(node_count):
        nodes.append(
            {
                "id": i,
                "label": possible_entities[entity_index],
                "name": f"{possible_entities[entity_index]}_{i}",
            }
        )
        entity_index = (entity_index + 1) % len(possible_entities)
    return nodes


# We take  advantage of the fact that node ids are incremental to make relationship
# Relationship are composed with neighboors
# 0-1, 1-2, 2-3, ... n-0, 0-2, 1-3, n-1
# until the number of edge requested is fullfiled
def generate_edges(node_count: int, edge_count: int) -> list[dict]:
    edges = []
    for i in range(edge_count):
        node_src = i % node_count
        node_dest = (node_src + 1 + (math.trunc(i / node_count))) % node_count
        edges.append({"src": node_src, "dest": node_dest, "type": "REL"})

    return edges


def generate_sample_data(node_count: int, edge_count: int, destination_folder: str):
    """Write `node_count` amount of node into `destination_folder`/nodes.csv.\n
    A node is: id,label,name.

    Write `edge_count` amount of edges into `destination_folder`/edges.csv.\n
    An edge is: src,dst,type. src and dst are ids of nodes.

    the program will exit(1), if the number of edges exceed the maximum number
    of undirected edges that can be created without creating duplicate (n(n-1))/2.

    Args:
        node_count (int): Number of nodes
        edge_count (int): Number of edges
        destination_folder (str): folder that will contain nodes.csv and edge.csv files
    """
    max_edge_count = math.trunc(node_count * (node_count - 1) / 2)
    if max_edge_count < edge_count:
        print(
            f"Too much edges. Maximum number of edges is {max_edge_count}. {edge_count} requested"
        )
        exit(1)

    print("Generating nodes ...")
    nodes = generate_nodes(node_count)
    print("Nodes generated.")

    print("Generating edges ...")
    edges = generate_edges(node_count, edge_count)
    print("Edges generated.")

    print("Writing files ...")
    with open(f"{destination_folder}/nodes.csv", "w", newline="") as node_file:
        node_writer = csv.DictWriter(node_file, nodes[0].keys())
        node_writer.writeheader()
        node_writer.writerows(nodes)

    with open(f"{destination_folder}/edges.csv", "w", newline="") as edge_file:
        writer = csv.DictWriter(edge_file, edges[0].keys())
        writer.writeheader()
        writer.writerows(edges)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--out",
        metavar="Destination folder",
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--nodes",
        metavar="Node count",
        type=int,
        required=True,
    )
    arg_parser.add_argument(
        "--edges",
        metavar="Edge count",
        type=int,
        required=True,
    )
    args = arg_parser.parse_args()

    generate_sample_data(args.nodes, args.edges, args.out)
