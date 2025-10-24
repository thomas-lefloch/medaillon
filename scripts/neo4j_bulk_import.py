import pandas as pd
from neo4j import GraphDatabase
import os

# TODO: arg parse


def to_csv():
    nodes = pd.read_parquet("data/silver/nodes.parquet")
    nodes.to_csv("data/gold/nodes.csv", index=False)

    edges = pd.read_parquet("data/silver/edges/")
    edges = edges.drop("shard", axis="columns")
    edges.to_csv("data/gold/edges.csv", index=False)


def import_into_neo4j():

    node_id_unique_constraint = """
        CREATE CONSTRAINT unique_node_id FOR (n:Node) REQUIRE n.id IS UNIQUE
    """

    load_nodes = """
        LOAD CSV WITH HEADERS FROM 'file:///nodes.csv'
        AS row
        CALL (row) {
            CREATE (:Node:$(row.label) {id: toInteger(row.id), name: row.name, label: row.label})
        } IN TRANSACTIONS
    """

    load_edges = """
        LOAD CSV WITH HEADERS FROM 'file:///edges.csv'
        AS row
        CALL (row) {
            MATCH (src{id: toInteger(row.src)})
            MATCH (dst{id: toInteger(row.dest)})
            CREATE (src)-[:$(row.type)]->(dst)
        } IN TRANSACTIONS
    """

    with GraphDatabase.driver(os.environ["NEO4J_URI"]) as driver:
        with driver.session() as session:
            res = session.run(node_id_unique_constraint).to_eager_result()
            print(
                res.summary.counters.constraints_added,
                "unique constraint on node.id added",
            )

            res = session.run(load_nodes).to_eager_result()
            print(res.summary.counters.nodes_created, "nodes created.")

            res = session.run(load_edges).to_eager_result()
            print(res.summary.counters.relationships_created, "edges created.")
