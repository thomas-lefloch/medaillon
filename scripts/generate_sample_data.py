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
# TODO: ajout None
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
    # arg_parser.add_argument(
    #     "--edges",
    #     metavar="Edge count",
    #     help="Nombre d'edge à créer",
    #     type=int,
    #     required=True,
    # )
    args = arg_parser.parse_args()

    nodes = generate_nodes(args.nodes)

    with open(f"{args.out}/nodes.csv", "w", newline="") as nodes_file:
        node_writer = csv.DictWriter(nodes_file, nodes[0].keys())
        node_writer.writeheader()
        node_writer.writerows(nodes)
