python <<EOF
import pandas as pd
import os

nodes = pd.read_parquet("data/silver/nodes.parquet")
nodes = nodes.rename({"id": "id:ID", "label": ":LABEL"}, axis="columns")
nodes.to_csv("data/gold/nodes.csv", index=False)

edges = pd.read_parquet("data/silver/edges/")
edges = edges.rename({"src": ":START_ID", "dest": ":END_ID", "type": ":TYPE"}, axis="columns").drop("shard", axis="columns")
edges.to_csv("data/gold/edges.csv", index=False)
EOF

docker compose run --rm\
    graph_db bin/neo4j-admin database import full\
    --id-type integer\
    --nodes //import/nodes.csv\
    --relationships //import/edges.csv\
    --overwrite-destination\
    