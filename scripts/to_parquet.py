from pyarrow import csv, parquet


# TODO: passer des paramêtres --src et --dst pour les dossier parents des fichiers
nodes = csv.read_csv("data/raw/nodes.csv")
parquet.write_table(nodes, "data/bronze/nodes.parquet")

edges = csv.read_csv("data/raw/edges.csv")
parquet.write_table(edges, "data/bronze/edges.parquet")