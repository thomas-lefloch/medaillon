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
# src,dst,type
# 0,123,REL
# 1,456,REL

# TODO: ajout doublon
# TODO: ajout valeur None

import math


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


import argparse
import csv

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--out",
        metavar="Destination folder",
        help="dossier d'écriture des données",
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--nodes",
        metavar="Node count",
        help="Nombre de node à créer",
        type=int,
        required=True,
    )
    arg_parser.add_argument(
        "--edges",
        metavar="Edge count",
        help="Nombre d'edge à créer",
        type=int,
        required=True,
    )
    args = arg_parser.parse_args()

    max_edge_count = math.trunc(args.nodes * (args.nodes - 1) / 2)
    if max_edge_count < args.edges:
        print(
            f"Too much edges. Maximum number of edges is {max_edge_count}. {args.edges} requested"
        )
        exit(1)

    print("Generating nodes ...")
    nodes = generate_nodes(args.nodes)
    print("Nodes generated.")
    
    print("Generating edges ...")
    edges = generate_edges(args.nodes, args.edges)
    print("Edges generated.")

    print("Writing files ...")
    with open(f"{args.out}/nodes.csv", "w", newline="") as node_file:
        node_writer = csv.DictWriter(node_file, nodes[0].keys())
        node_writer.writeheader()
        node_writer.writerows(nodes)

    with open(f"{args.out}/edges.csv", "w", newline="") as edge_file:
        writer = csv.DictWriter(edge_file, edges[0].keys())
        writer.writeheader()
        writer.writerows(edges)
