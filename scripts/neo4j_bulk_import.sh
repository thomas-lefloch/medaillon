python <<EOF
import pandas as pd

nodes = pd.read_parquet("data/silver/nodes.parquet")
nodes = nodes.rename({"id": "id:ID"})
nodes.to_csv("data/gold/nodes.csv", index=False)

edges = pd.read_parquet("data/silver/edges/")
edges = edges.rename({"src": ":START_ID", "dest": ":END_ID"})
edges.to_csv("data/gold/edges.csv", index=False)
EOF