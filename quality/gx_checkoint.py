# Question:
# - avant ou après la transformation en parquet ?? apres (début silver)
# - Pourquoi gx_checkpoint ?? Great Expectation (biblio)
# - Que fait le script si il detecte des erreurs / données non valables ?
# suppression

# - Quelles programmes pour neo4j_bulk_import ?
# requete cypher en python dans .sh


# return the list of uniques nodes and a set of all unique ids
def filter_nodes_without_unique_id(nodes: list[dict]) -> list[dict]:
    uniques_ids = set()
    valid_nodes = []
    for node in nodes:
        if node["id"] in uniques_ids:
            continue
        valid_nodes.append(node)
        uniques_ids.add(node["id"])

    return valid_nodes


# return the list of edges that don't have None for src and dest
def filter_edges_with_missing_ids(edges: list[dict]) -> list[dict]:
    valid_edges = []
    for edge in edges:
        if edge["src"] != None and edge["dest"] != None:
            valid_edges.append(edge)
    return valid_edges
